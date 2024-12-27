# -------------------------------------------------------------------------
# -------------------------------------------------------------------------

# VALORACION PROMOCOMERCIO ------------------------------------------------

# -------------------------------------------------------------------------
# -------------------------------------------------------------------------

setwd("C:/Users/ivan.villalba/Documents/R Scripts and projects/VALORACION P70")

# LIBRERIAS ---------------------------------------------------------------

require(pacman)
p_load(tidyverse, lubridate, stringi, readxl, openxlsx, DBI, odbc,magrittr)
options(scipen=999)

#source("CREDITOS AQ/union tablas.R")
# -------------------------------------------------------------------------

end_date <-seq(as.Date(paste(year(Sys.Date()), "-02-01", sep = "")), 
               as.Date(paste(year(Sys.Date()) + 1, "-01-01", sep = "")), 
               by = "month") - 1
# QUERIES Y LECTURA -------------------------------------------------------


Conexion_comite <- DBI::dbConnect(odbc::odbc(),Driver = "SQL Server",Server = '192.168.50.38\\DW_FZ',  
                                       Trusted_Connection = "yes",timeout = 1000, database = "DW_FZ")

QUERY <- "SELECT id_fecha,
                  credito,
                  id_fecha_desembolso,
                  valor_desembolso,
                  numero_dias_mora,
                  saldo_capital,
                  saldo_interes,
                  saldo_seguros,
                  saldo_cargo_1,
                  saldo_cargo_2,
                  saldo_cargo_3,
                  saldo_mora,
                  valor_canc_capital_intereses_seguros,
                  tipo_producto,
                  valor_garantia,
		              ROW_NUMBER() over(partition by credito order by id_fecha asc) as orden
	        FROM DW_FZ.dbo.Fact_Cartera_Mes"

#QUERY_REC <- "SELECT credito, id_fecha_efe_mov, cod_trans, valor_pago_capital FROM DW_FZ.dbo.Fact_Recaudo WHERE cod_trans = 88"

# CRUCE CARTERA DIA -------------------------------------------------------
fecha_actual = str_replace_all(end_date[month(Sys.Date())-1],'-','')
fechas_cierre = str_c(str_replace_all(end_date[month(Sys.Date())-1] + 1:5,'-',''),collapse = ',')


query_dia <-  str_glue("SELECT saldo_capital,
                    tipo_producto,
                    credito,
                    valor_pago_ttl_canc
                FROM [DW_FZ].[dbo].[Fact_Cartera_Dia] 
                where tipo_producto in (70,71) and saldo_capital > 0 and id_fecha in ({fechas_cierre}) and
                      ind_cierre=1")

# QUERY LTV CAROLINA ------------------------------------------------------

query_LTV <- str_glue("select l.credito, l.id_fecha, l.tipo_producto,l.conteo_garantias, l.LTV, l.valor_garantia_final,
                    coalesce(l.libre_inversion_o_vehicular,'') as libre_inversion_o_vehicular, coalesce(rr.marc_desc,'') as marca_refi_restru
                    from Cartera175.dbo.ltv l
                    left join (select t.credito,marc_desc from (
                    select *,ROW_NUMBER() over (partition by credito order by id_fecha) as n,
                    case when marcacion = 'R' then 'Refinanciacion'
                        else 'Reestructuracion' end as marc_desc
                    from[DW_FZ].[dbo].[Fact_Cartera_Marcacion_RFS] where marcacion != 'S') as t
                    where n=1) rr on l.credito = rr.credito
                    where l.tipo_producto = 70 and l.id_fecha ={fecha_actual}")

# -------------------------------------------------------------------------


Tabla_comite <- DBI::dbGetQuery(conn = Conexion_comite, statement = QUERY)

#Tabla_rec <- DBI::dbGetQuery(conn = Conexion_comite, statement = QUERY_REC)

tabla_dia <- DBI::dbGetQuery(conn = Conexion_comite, statement = query_dia)

tabla_ltv <- DBI::dbGetQuery(conn = Conexion_comite, statement = query_LTV)


## DEPURACION DE CREDITOS

valores_nulos <- c(30329,50935,54468,58747, 215323, 56226, 57115)
intereses_negativos <- c(54945,57617,78439,81431,84916,84920,96177,102267,132338,206465)

# -------------------------------------------------------------------------

#rm(list = ls()[!ls() %in% c('Tabla_comite')])


# CARTERA PROMOCOMERCIO ---------------------------------------------------
Promocomercio <- Tabla_comite %>% filter(tipo_producto==70) %>%
                                  mutate(Mes = str_sub(id_fecha,start=5,end=6), Año = str_sub(id_fecha,start=1,end=4))

# -------------------------------------------------------------------------

# PROMOCOMERCIO - TASA DE RECUPERACION ------------------------------------

saldo_capital <- Promocomercio %>% group_by(credito) %>% 
                  summarise(Fecha_desembolso = mean(as.numeric(id_fecha_desembolso)),
                            fecha_inicio_p70 = min(id_fecha),
                            fecha_vigencia = max(id_fecha),
                            Duracion_p70 = n(),
                            Duracion_p10 = min(orden)) %>% arrange(fecha_vigencia)

### CRUCES TEMPORALES - REGISTROS DE POSICION
ultimo_registro_mora <- Promocomercio %>% group_by(credito) %>%
                                          slice((which.max(id_fecha))) %>% 
                                          summarise(dias_mora_actual = numero_dias_mora)

primer_registro_sc <- Promocomercio %>% group_by(credito) %>%
                                        slice((which.min(id_fecha))) %>% 
                                        summarise(Saldo_capital_inicial = saldo_capital)

ultimo_registro_sc <- Promocomercio %>% group_by(credito) %>%
                                        slice((which.max(id_fecha))) %>% 
                                        summarise(Saldo_capital_final = saldo_capital)

saldo_capital %<>% left_join(primer_registro_sc) %>% 
                    left_join(ultimo_registro_sc) %>% 
                    left_join(ultimo_registro_mora) 


# -------------------------------------------------------------------------

# AJUSTE VALORES CERO -----------------------------------------------------

creditos_sin_sc <- saldo_capital %>% filter(Saldo_capital_inicial==0)

panterior <- Tabla_comite %>% filter(credito %in% creditos_sin_sc$credito) %>%
                              group_by(credito) %>%
                              slice((which.max(id_fecha)-1):which.max(id_fecha)) %>%
                              arrange(credito) %>% filter(tipo_producto != 70) %>%
                              mutate(capital_ant = saldo_capital)%>%
                              select(credito,capital_ant)

saldo_capital %<>% left_join(panterior) %>% 
                  mutate(Saldo_capital_inicial = ifelse(Saldo_capital_inicial == 0 & Duracion_p70 == 1,capital_ant,Saldo_capital_inicial)) %>%
                  select(-capital_ant)


# -------------------------------------------------------------------------


# VALORACION - TASA RECUPERACION ANUAL ------------------------------------

valoracion <- saldo_capital %>%
  mutate(prop_recuperado = ifelse((Saldo_capital_inicial-Saldo_capital_final) < 0, 0,(Saldo_capital_inicial - Saldo_capital_final)/Saldo_capital_inicial),
         tasa_recuperacion_anual = (prop_recuperado/(Duracion_p70+1))*12)

# -------------------------------------------------------------------------

# BENEFICIO COMPARATIVO --------------------------------------------

#VAL_PD70 <- valoracion %>% mutate(Precio_rec = (Saldo_capital_inicial-Saldo_capital_final)/(1+tasa_recuperacion_anual*(Duracion_p70/12)),
#                      Diferencia = ifelse(`Precio de compra` == 0, 1, Precio_rec /`Precio de compra` )) 

# FIN VALORACION ----------------------------------------------------------

TABLA_DIA <- valoracion %>% left_join(tabla_dia, by = "credito") %>% 
            left_join(tabla_ltv %>% filter(id_fecha==fecha_actual)%>%
                                    select(credito,LTV,valor_garantia_final,libre_inversion_o_vehicular,marca_refi_restru), by='credito') #%>%
            left_join(tabla_ltv %>% filter(id_fecha==20241031) %>% select(credito,garantia_anterior=valor_garantia_final,LTV_ant = LTV),by='credito')


TABLA_VAL <- TABLA_DIA 

VALORACION_FINAl <- TABLA_VAL %>%
  mutate(Valoracion_pd = case_when(
    tasa_recuperacion_anual > 0 & LTV > 1 ~ 1*(1+ tasa_recuperacion_anual)*(1+(LTV/(Duracion_p70))),
    tasa_recuperacion_anual == 0 & LTV >1 ~ 1*(1+0.75*(LTV)),
    TRUE ~ 1*(1+tasa_recuperacion_anual)))


# -------------------------------------------------------------------------

# CODIGOS DE CMPROBACION DE REGISTROS -------------------------------------

#Tabla_comite %>% filter(credito == 157949) %>% view()

#saldo_capital %>% filter(credito == 143803) %>% view()

#VALORACION_FINAl %>% filter(credito == 143803) %>% view()

# -------------------------------------------------------------------------

#VALORACION_FINAL_MES <- VALORACION_FINAl %>% filter(fecha_vigencia == 20240930)

# ESCRITURA TABLA DE VALORACION P70 ---------------------------------------

setwd("C:/Users/ivan.villalba/Documents/R Scripts and projects/VALORACION P70/ARCHIVO PARA VALORACION/DF_VALORACION_GENERAL/")

write_csv2(VALORACION_FINAl,paste0('VALORACION_PRUEBA_pd70_total_',fecha_actual,'.csv'))

