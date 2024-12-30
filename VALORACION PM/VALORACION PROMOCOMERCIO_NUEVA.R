# -------------------------------------------------------------------------
# -------------------------------------------------------------------------

# VALORACION PROMOCOMERCIO ------------------------------------------------

# -------------------------------------------------------------------------
# -------------------------------------------------------------------------

setwd("//192.168.40.9/garaje$/BackUps_Analitica/BACKUP IVAN/ORGANIGRAMA/VALORACION PM/ARCHIVO PARA VALORACION")
# LIBRERIAS ---------------------------------------------------------------

require(pacman)
p_load(tidyverse, lubridate, stringi, readxl, openxlsx, DBI, odbc, magrittr)
## QUITAR NOTACION CIENTIFICA
options(scipen=999)
# -------------------------------------------------------------------------

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

# CRUCE CARTERA DIA -------------------------------------------------------

### CONFIGURACION FECHAS DINAMICAS

end_date <-seq(as.Date(paste(year(Sys.Date()), "-02-01", sep = "")), 
               as.Date(paste(year(Sys.Date()) + 1, "-01-01", sep = "")), 
               by = "month") - 1

fecha_actual = str_replace_all(end_date[month(Sys.Date())-1],'-','')
fechas_cierre = str_c(str_replace_all(end_date[month(Sys.Date())-1] + 1:5,'-',''),collapse = ',')
fechas_cierre
####

# QUERY METADIARIA --------------------------------------------------------
query_dia <-  str_glue("SELECT saldo_capital,
                            tipo_producto,
                            credito,
                            valor_pago_ttl_canc
                        FROM [DW_FZ].[dbo].[Fact_Cartera_Dia] 
                        where tipo_producto in (70,71) and saldo_capital > 0 and id_fecha in ({fechas_cierre}) and
                              ind_cierre=1")


# QUERY LTV ------------------------------------------------------

query_LTV <- str_glue("select l.credito, l.id_fecha, l.tipo_producto,l.conteo_garantias, l.LTV, l.valor_garantia_final,
                        coalesce(l.libre_inversion_o_vehicular,'') as libre_inversion_o_vehicular, coalesce(rr.marc_desc,'') as marca_refi_restru
                        from Cartera175.dbo.ltv l
                        left join (select t.credito,marc_desc from (
                        select *,ROW_NUMBER() over (partition by credito order by id_fecha) as n,
                        case when marcacion = 'R' then 'Refinanciacion'
                            else 'Reestructuracion' end as marc_desc
                        from[DW_FZ].[dbo].[Fact_Cartera_Marcacion_RFS] where marcacion != 'S') as t
                        where n=1) rr on l.credito = rr.credito
                        where l.id_fecha in ({fecha_actual})")

# -------------------------------------------------------------------------


Tabla_comite <- DBI::dbGetQuery(conn = Conexion_comite, statement = QUERY)

tabla_dia <- DBI::dbGetQuery(conn = Conexion_comite, statement = query_dia)

tabla_ltv <- DBI::dbGetQuery(conn = Conexion_comite, statement = query_LTV)


# ## DEPURACION DE CREDITOS
# #,206465,180102
# 
# valores_nulos <- c(30329,50935,54468,58747, 215323, 56226, 57115)
# intereses_negativos <- c(54945,57617,78439,81431,84916,84920,96177,102267,132338,206465)
# -------------------------------------------------------------------------

# CARTERA PROMOCOMERCIO ---------------------------------------------------
### BASE PRINCIPAL
Promocomercio <- Tabla_comite %>% filter(tipo_producto==70) %>%
                                  mutate(Mes = str_sub(id_fecha,start=5,end=6), Año = str_sub(id_fecha,start=1,end=4))

### GENERACION DATOS UTILES
#IMPORTANTE: ESTA FASE SE REALIZA DE TAL MANERA POR LOS SUPUESTOS BRINDADOS A PARTIR DEL TIPO DE DATOS

fechas_iniciales <- Promocomercio %>% group_by(credito) %>% 
                                  summarise(Fecha_desembolso = min(as.numeric(id_fecha_desembolso)),
                                            fecha_inicio_p70 = min(id_fecha),
                                            fecha_vigencia = max(id_fecha),
                                            Duracion_p70 = n(),
                                            Duracion_p10 = min(orden)) %>% arrange(fecha_vigencia)

### VALORES INICIALES Y ACTUALES

Valores_actuales <- Promocomercio %>% select(id_fecha,credito,numero_dias_mora,saldo_capital,saldo_interes,saldo_seguros,
                         saldo_cargo_1,saldo_cargo_2,saldo_cargo_3,saldo_mora) %>% group_by(credito) %>%
                    slice(which.max(id_fecha)) %>% setNames(paste0(names(.),'_actual'))

Valores_inciales <- Promocomercio %>% select(id_fecha,credito,numero_dias_mora,saldo_capital,saldo_interes,saldo_seguros,
                         saldo_cargo_1,saldo_cargo_2,saldo_cargo_3,saldo_mora) %>% group_by(credito) %>%
                    slice(which.min(id_fecha)) %>% setNames(paste0(names(.),'_inicial'))

### CONCATENACION 
BBB <- fechas_iniciales %>% left_join(Valores_inciales,by=c('credito'='credito_inicial')) %>%
                          left_join(Valores_actuales,by=c('credito'='credito_actual')) %>%
                          mutate(Saldo_exposicion_inicial =
                                   saldo_capital_inicial + saldo_interes_inicial + saldo_seguros_inicial + 
                                   saldo_cargo_1_inicial + saldo_cargo_2_inicial +  saldo_cargo_3_inicial,
                                 Saldo_exposicion_actual =
                                   saldo_capital_actual + saldo_interes_actual + saldo_seguros_actual + 
                                   saldo_cargo_1_actual + saldo_cargo_2_actual + saldo_cargo_3_actual)


# ESQUEMA DE SOPORTE ------------------------------------------------------

## SE TOMA EL ULTIMO REGISTRO DE VALOR EN CASO DE QUE EN PRODUCTO 70 INICIE EN CERO.
## ESTO SE RRALIZA CON EL FIN DE TENER EN CUENTA LOS CREDITOS QUE SE CANCELAN INMEDIATAMENTE
## LUEGO DE SER COMPRADOS

creditos_sin_sc <- BBB %>% filter(Saldo_exposicion_inicial==0)

panterior <- Tabla_comite %>% filter(tipo_producto !=70 & credito %in% creditos_sin_sc$credito) %>%
  group_by(credito) %>%
  slice((which.max(id_fecha))) %>%
  arrange(credito) %>% filter(tipo_producto != 70) %>%
  mutate(capital_ant = saldo_capital + saldo_interes + saldo_seguros +
                      saldo_cargo_1 + saldo_cargo_2 + saldo_cargo_3,
         saldo_ant = saldo_capital)%>%
  select(credito,capital_ant,saldo_ant)

BBB %<>% left_join(panterior) %>% 
  mutate(Saldo_exposicion_inicial = ifelse(Saldo_exposicion_inicial == 0 & Duracion_p70 == 1,capital_ant,Saldo_exposicion_actual),
         saldo_capital_inicial = ifelse(saldo_capital_inicial== 0 & Duracion_p70 == 1,saldo_ant,saldo_capital_inicial) ) %>%
  select(-c(capital_ant,saldo_ant))


# GENERACION DE CALCULOS PRINCIPALES --------------------------------------

## ALTERNATIVA 1 - SALDO EXPUESTO SIN MORA

DF_SALDOS <- BBB %>% 
        select(credito,id_fecha_inicial,id_fecha_actual,Duracion_p70,Duracion_p10,saldo_capital_inicial,
               saldo_capital_actual, Saldo_exposicion_inicial,Saldo_exposicion_actual,saldo_mora_inicial,
               saldo_mora_actual) %>% 
        mutate(prop_recuperado_exposicion = (Saldo_exposicion_inicial - Saldo_exposicion_actual)/Saldo_exposicion_inicial,
               tasa_recuperacion_anual = (prop_recuperado_exposicion/(Duracion_p70+1))*12)


#### ALTERNATIVA 2 - SALDO CAPITAL Y SALDO MORA

DF_SALDOS2 <- BBB %>% 
  select(credito,id_fecha_inicial,id_fecha_actual,Duracion_p70,Duracion_p10,saldo_capital_inicial,saldo_capital_actual,
                           Saldo_exposicion_inicial,Saldo_exposicion_actual,saldo_mora_inicial,saldo_mora_actual) %>% 
  mutate(prop_recuperado_mora = ifelse((saldo_mora_inicial-saldo_mora_actual) < 0, 0,(saldo_mora_inicial - saldo_mora_actual)/saldo_mora_inicial),
         tasa_recuperacion_anual_mora = (prop_recuperado_mora/(Duracion_p70+1))*12,
         prop_recuperado_capital = ifelse((saldo_capital_inicial-saldo_capital_actual) < 0, 0,(saldo_capital_inicial - saldo_capital_actual)/saldo_capital_inicial),
         tasa_recuperacion_anual_capital = (prop_recuperado_capital/(Duracion_p70+1))*12)

## SINTESIS ALTERNATIVA 2
DF_SALDOS2 <- DF_SALDOS2 %>% mutate(tasa_recuperacion_anual = ifelse(is.nan(tasa_recuperacion_anual_mora),
                                                       tasa_recuperacion_anual_capital,
                                                       ifelse(is.nan(tasa_recuperacion_anual_capital),tasa_recuperacion_anual_mora,
                                                       ifelse(tasa_recuperacion_anual_mora == 0 & 
                                                                tasa_recuperacion_anual_capital ==0,0,
                                                              ifelse(tasa_recuperacion_anual_mora > tasa_recuperacion_anual_capital,
                                                                     tasa_recuperacion_anual_mora,tasa_recuperacion_anual_capital)))))

# -------------------------------------------------------------------------
### UNION TASAS

TABLA_VALORACION <- DF_SALDOS %>% select(credito,id_fecha_inicial,id_fecha_actual,
                     Duracion_p10,Duracion_p70, tasa_recuperacion_anual_exp = tasa_recuperacion_anual) %>% 
                    left_join(DF_SALDOS2 %>% select(credito,tasa_recuperacion_anual,
                                                    tasa_recuperacion_anual_capital),by='credito') %>%
                    left_join(tabla_ltv %>%
                                select(credito,LTV,valor_garantia_final,
                                       libre_inversion_o_vehicular,marca_refi_restru), by='credito') %>%
                    left_join(tabla_dia, by = "credito")

### DF GENERAL DE CALCULO

VALORACION_PD70 <- TABLA_VALORACION

# PROMOCOMERCIO - TASA DE RECUPERACION ------------------------------------

# -------------------------------------------------------------------------

#setwd("C:/Users/ivan.villalba/Documents/R Scripts and projects/VALORACION P70/ARCHIVO PARA VALORACION/")
#options(scipen=999,digits = 20)

### CARGA DE ARCHIVOS INDISPENSABLES

# CARTERA PROMOCOMERCIO - SUMINISTRADA POR CONTABILIDAD
CARTERA_PD70 <- read_excel('BASE_CREDITOS_PM/Base creditos PM Noviembre_24.xlsx', skip = 3) %>% filter(!is.na(Credito))

# CARTERA CON TASA DE RECUPERACION - ESQUEMA ELABORADO PREVIAMENTE


VALORACION_PD70 %<>%  mutate(Valoracion_pd = case_when(
                              tasa_recuperacion_anual > 0 & LTV <= 0.8 ~ 1*(1+ tasa_recuperacion_anual)*(1+(1/LTV)*(1/Duracion_p70)),
                              tasa_recuperacion_anual == 0 & LTV <= 0.6 ~ 1*(1+0.6*(1/LTV)),
                              TRUE ~ 1*(1+tasa_recuperacion_anual)),
                             Valoracion_pd_mod = case_when(
                              tasa_recuperacion_anual_exp > 0 & LTV <= 0.8 ~ 1*(1+ tasa_recuperacion_anual_exp)*(1+(1/LTV)*(1/Duracion_p70)),
                              tasa_recuperacion_anual_exp == 0 & LTV <= 0.6 ~ 1*(1+0.6*(1/LTV)),
                              TRUE ~ 1*(1+tasa_recuperacion_anual_exp)),
                             Valoracion_pd_cap = case_when(
                               tasa_recuperacion_anual_capital > 0 & LTV <= 0.8 ~ 1*(1+ tasa_recuperacion_anual_capital)*(1+(1/LTV)*(1/Duracion_p70)),
                               tasa_recuperacion_anual_capital == 0 & LTV <= 0.6 ~ 1*(1+0.6*(1/LTV)),
                               TRUE ~ 1*(1+tasa_recuperacion_anual_capital)))


DF <- CARTERA_PD70 %>%
  left_join(VALORACION_PD70,
            by = c('Credito'='credito')) %>%
  rename('Saldo Actual'='Suma de Importe base') %>%
  mutate(Precio_y_valoracion=`Precio de compra`* Valoracion_pd,
         Precio_y_valoracion_mod= `Precio de compra`*Valoracion_pd_mod,
         Precio_y_valoracion_capital= `Precio de compra`*Valoracion_pd_cap)


# IDENTIFICACION CREDITOS ACTIVOS E INACTIVOS -----------------------------

### ACTIVOS
DF_ZZ <- DF %>% filter(!is.na(valor_pago_ttl_canc))


### CREDITOS INACTIVOS
DF_X <- DF %>% filter(is.na(valor_pago_ttl_canc))


# CREACION RESUMENES Y AJUSTE POR CREDITOS INACTIVOS ----------------------

### AGRUPACION CREDITOS ACTIVOS - VALORACION CONSOLIDADA

tabla_valoracion <- DF_ZZ  %>% filter(`Saldo Actual`>0.0001) %>% group_by(tipo_producto) %>% summarise(Creditos = n(),
                                             Deuda_total = sum(valor_pago_ttl_canc, na.rm = T),
                                             Saldo_a_capital = sum(`Saldo Actual`,na.rm = T),
                                             Precio_de_compra= sum(`Precio de compra`, na.rm = T),
                                             Valoracion = sum(Precio_y_valoracion,na.rm = T),
                                             Valoracion_mod = sum(Precio_y_valoracion_mod,na.rm = T),
                                             Valoracion_capital = sum(Precio_y_valoracion_capital,na.rm = T)) %>%
                                  mutate(p = Valoracion/Precio_de_compra-1,
                                         p_mod = Valoracion_mod/Precio_de_compra-1,
                                         p_cap = Valoracion_capital/Precio_de_compra -1)

### AGRUPACION CREDITOS CANCELADOS ANTES DE CIERRE ACTUAL

## CREDITOS INDIVIDUALES
residuo <- DF_X %>% filter(`Saldo Actual`>0.0001) %>%
                select(Credito,valor_pago_ttl_canc,`Saldo Actual`,`Precio de compra`,Precio_y_valoracion)

## AGRUPaACION VALORES
residuo_agrupado <-  residuo %>% summarise(Creditos = n(), 
                                            Deuda_total = sum(valor_pago_ttl_canc, na.rm = T),
                                            Saldo_a_capital = sum(`Saldo Actual`,na.rm = T),
                                            Precio_de_compra= sum(`Precio de compra`, na.rm = T),
                                            Valoracion = sum(Precio_y_valoracion,na.rm = T)) %>%
                                 mutate(p =Valoracion/Precio_de_compra-1)

# DISCRIMINACION DE TABLAS POR TIPO PRODUCTO ------------------------------

### TABLA CREDITOS 70                  
tabla_p70 <- DF_ZZ %>% filter(tipo_producto==70) %>% select(Credito,'Saldo a capital'=`Saldo Actual`, Compra = `Precio de compra`, Precio_y_valoracion_mod,Precio_y_valoracion,Precio_y_valoracion_capital) %>%
                                                  mutate(VG = Precio_y_valoracion_mod/Compra -1  ,
                                                         'Valoracion con garantia' = ifelse(is.na(VG),0,VG)) %>% select(-c(Precio_y_valoracion_mod,VG))
                                                        
### TABLA CREDITOS 71
tabla_p71 <- DF_ZZ %>% filter(tipo_producto==71) %>% select(Credito,'Saldo a capital'=`Saldo Actual`, Compra = `Precio de compra`) %>%
                                                  mutate('Valoracion con garantia' = 0)


# AGRUPACION EN LISTA -----------------------------------------------------

### SE EXPORTA UN EXCEL CON VARIAS HOJAS QUE CONTIENEN LOS ELEMENTOS DE PRESENTACION: LISTA COMPLETA, TABLAS DISCRIMINADAS y RESUMENES

Lista_tablas_valoracion = list(Cartera = CARTERA_PD70, 
                               Resumen = tabla_valoracion2,
                               residuo=residuo, 
                               Vigentes_70 = tabla_p70, 
                               Vigentes_71 = tabla_p71)

# ESCRITURA COMPONENTES  --------------------------------------------------
### CAMBIAR EL NOMBRE PARA EVITAR SOBRE ESCRITURA
meses <- c("Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", 
           "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre")

openxlsx::write.xlsx(Lista_tablas_valoracion,paste0('Resumen y créditos vigentes ',meses[month(Sys.Date())-1],'.xlsx'))
