# -------------------------------------------------------------------------

# -------------------------------------------------------------------------


# MATRIZ DE RODAMIENTOS Pd 70 ---------------------------------------------


# -------------------------------------------------------------------------


# -------------------------------------------------------------------------

setwd("C:/Users/ivan.villalba/Documents/R Scripts and projects/VALORACION P70")
# LIBRERIAS ---------------------------------------------------------------

require(pacman)
p_load(tidyverse, lubridate, stringi, readxl, openxlsx, DBI, odbc,magrittr)
options(scipen=999)


# -------------------------------------------------------------------------
# QUERIES Y LECTURA -------------------------------------------------------

Conexion_comite <- DBI::dbConnect(odbc::odbc(),Driver = "SQL Server",Server = '192.168.50.38\\DW_FZ',  
                                  Trusted_Connection = "yes",timeout = 1000, database = "DW_FZ")

QUERY <- "SELECT id_fecha,
                  credito,
                  tipo_id_deudor,
                  id_fecha_desembolso,
                  id_fecha_primera_factura,
                  valor_desembolso,
                  numero_dias_mora,
                  saldo_capital,
                  saldo_interes,
                  saldo_seguros,
                  saldo_mora,
                  valor_canc_capital_intereses_seguros,
                  tipo_producto,
                  base_de_provision,
                  total_deuda,
                  valor_garantia,
		              ROW_NUMBER() over(partition by credito order by id_fecha asc) as orden
	        FROM Fact_Cartera_Mes"

QUERY_REC <- "SELECT credito, id_fecha_efe_mov, cod_trans, valor_pago_capital FROM DW_FZ.dbo.Fact_Recaudo WHERE cod_trans = 88"

Tabla_comite <- DBI::dbGetQuery(conn = Conexion_comite, statement = QUERY)

Tabla_rec <- DBI::dbGetQuery(conn = Conexion_comite, statement = QUERY_REC)


# -------------------------------------------------------------------------
## DEPURACION DE CREDITOS
valores_nulos <- c(30329,50935,54468,58747, 215323, 56226, 57115)
intereses_negativos <- c(54945,57617,78439,81431,84916,84920,96177,102267,132338,206465)
# -------------------------------------------------------------------------

#rm(list = ls()[!ls() %in% c('Tabla_comite')])
# CARTERA PROMOCOMERCIO ---------------------------------------------------

cod_88 <- Tabla_rec %>% select(credito,cod_trans) %>% distinct()

Promocomercio <- Tabla_comite %>% filter(tipo_producto==70) %>%
  left_join(cod_88) %>%
  mutate(Mes = str_sub(id_fecha,start=5,end=6), Año = str_sub(id_fecha,start=1,end=4))

# -------------------------------------------------------------------------
# -------------------------------------------------------------------------

# RODAMIENTO P70 ----------------------------------------------------------

# -------------------------------------------------------------------------



ROD_70 <- Promocomercio %>% mutate(Mes = str_sub(id_fecha,start=5,end=6), Año = str_sub(id_fecha,start=1,end=4)) %>%
                            mutate(icvs = case_when(numero_dias_mora == 0 ~ 'ICV0',
                                                    between(numero_dias_mora, 0,30)~ 'ICV30',
                                                    between(numero_dias_mora, 31,60) ~ 'ICV60',
                                                    between(numero_dias_mora, 61,90) ~ 'ICV90',
                                                    between(numero_dias_mora, 91,120) ~ 'ICV120',
                                                    between(numero_dias_mora, 121,150) ~ 'ICV150',
                                                    between(numero_dias_mora, 151,180) ~ 'ICV180',
                                                    between(numero_dias_mora, 181,210) ~ 'ICV210',
                                                    between(numero_dias_mora, 211,240) ~ 'ICV240',
                                                    between(numero_dias_mora, 241,270) ~ 'ICV270',
                                                    between(numero_dias_mora, 271,300) ~ 'ICV300',
                                                    between(numero_dias_mora, 301,330) ~ 'ICV330',
                                                    between(numero_dias_mora, 331,360) ~ 'ICV360',
                                                    numero_dias_mora > 360  ~ 'ICV360+'))

### HISTORICO PARTICIPACION SALDO CAPITAL VENCIDO POR CATEGORIAS DE VENCIMIENTO

ROD_70_cash <- ROD_70 %>% group_by(Año,Mes,icvs) %>% summarise(n = sum(saldo_capital)) %>% ungroup(icvs) %>% mutate(p=n/sum(n)) %>%
  select(-n) %>% pivot_wider(names_from = icvs,values_from = p) %>% 
  select(Año,Mes,ICV0,ICV30,ICV60,ICV90,ICV120,ICV150,ICV180,ICV210,ICV240,ICV270,ICV300,ICV330,ICV360)

openxlsx::write.xlsx(ROD_70_cash,'PROPORCION DE SALDO A CAPITAL POR CATEGORIA DE VENCIMIENTO.xlsx')
# MATRIZ DE RODAMIENTOS ---------------------------------------------------


# CONFIGURACION T-1 -------------------------------------------------------

Matriz_t0 <- ROD_70 %>% select(Año,Mes,credito,icvs) %>% 
  filter(Año =='2021' & Mes =='08') %>% 
  select(-Año,-Mes) %>% mutate(unos = 1) %>%
  pivot_wider(names_from = icvs,values_from = unos) %>%
  select(c('credito','ICV0','ICV30','ICV60','ICV90',
           'ICV120','ICV150','ICV180','ICV210','ICV240',
           'ICV270','ICV300','ICV330','ICV360','ICV360+'))

creditos_t0 <- select(Matriz_t0,credito)

# CONFIGURACION T ---------------------------------------------------------

Matriz_t1 <- ROD_70 %>% select(Año,Mes,credito,icvs) %>% 
  filter(Año =='2021' & Mes =='09') %>% 
  select(-Año,-Mes) %>% mutate(unos = 1) %>%
  pivot_wider(names_from = icvs,values_from = unos) %>%
  select(c('credito','ICV0','ICV30','ICV60','ICV90',
           'ICV120','ICV150','ICV180','ICV210','ICV240',
           'ICV270','ICV300','ICV330','ICV360','ICV360+'))

creditos_t1 <- select(Matriz_t1,credito)

# -------------------------------------------------------------------------

### CREDITOS COMUNES (CREDITOS QUE ESTAN 'RODANDO')
creditos_comunes <- intersect(creditos_t1,creditos_t0)                    


# AJUSTE DE MATRICES ------------------------------------------------------

M0 <- Matriz_t0 %>% filter(credito %in% creditos_comunes$credito)

M0 <- M0 %>% lapply(function (x){
  unlist(lapply(x ,
                function (y) {
                  ifelse(is.na(y),0,y)}))}) %>% 
  as.data.frame() %>%
  arrange(credito)

M1 <- Matriz_t1 %>% filter(credito %in% creditos_comunes$credito)

M1 <- M1 %>% lapply(function (x){
  unlist(lapply(x ,
                function (y) {
                  ifelse(is.na(y),0,y)}))}) %>% 
  as.data.frame() %>%
  arrange(credito)

# -------------------------------------------------------------------------

# MULTIPLICACION MATRICIAL ------------------------------------------------

Mt0 <- M0 %>% select(-1) %>% data.matrix()
row.names(Mt0) <- M0$credito

Mt1 <- M1 %>% select(-1) %>% data.matrix() 
row.names(Mt1) <- M1$credito

## MATRIZ UNIDADES
MATRIZGOAT <- t(Mt0) %*% Mt1
## MATRIZ PROPORCIONES
MATRIZGOAT_p <- MATRIZGOAT / (MATRIZGOAT %*% data.matrix(rep(1,14)) %>% as.vector()) 

## VALIDACION TOTALES
MATRIZGOAT_p %*% data.matrix(rep(1,13)) 

# -------------------------------------------------------------------------

### ESCRITURA DATOS DE PRUEBA
openxlsx::write.xlsx(as.data.frame(MATRIZGOAT),'MATRIZ RODAMIENTOS UNIDADES Pd 70 JUN 2023_2024.xlsx',rowNames =T)
openxlsx::write.xlsx(as.data.frame(MATRIZGOAT_p),'MATRIZ RODAMIENTOS % Pd 70 JUN 2023_2024.xlsx', rowNames =T)

# FIN ---------------------------------------------------------------------
