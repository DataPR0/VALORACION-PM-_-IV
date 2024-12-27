
# -------------------------------------------------------------------------
### GENERACION DE ARCHIVOS PARA INFORME DE VALORACION: ELEMENTOS
# -------------------------------------------------------------------------


# VALORACION --------------------------------------------------------------

### DIRECTORIO DE TRABAJO
setwd("C:/Users/ivan.villalba/Documents/R Scripts and projects/VALORACION P70/ARCHIVO PARA VALORACION/")
options(scipen=999,digits = 20)

### CARGA DE ARCHIVOS INDISPENSABLES

# CARTERA PROMOCOMERCIO - SUMINISTRADA POR CONTABILIDAD
CARTERA_PD70 <- read_excel('Base creditos PM Noviembre_24.xlsx', skip = 3) %>% filter(!is.na(Credito))

# CARTERA CON TASA DE RECUPERACION - ESQUEMA ELABORADO PREVIAMENTE

archivos <- list.files('DF_VALORACION_GENERAL/',pattern = 'VALORACION_PRUEBA_pd70_total',full.names = T)

VALORACION_PD70 <- read_csv2(archivos[which.max(file.info(archivos)$mtime)])

VALORACION_PD70 %<>%  mutate(Valoracion_pd_mod = case_when(
                              tasa_recuperacion_anual > 0 & LTV <= 0.8 ~ 1*(1+ tasa_recuperacion_anual)*(1+(1/LTV)*(1/Duracion_p10)),
                              tasa_recuperacion_anual == 0 & LTV <= 0.6 ~ 1*(1+0.6*(1/LTV)),
                              TRUE ~ 1*(1+tasa_recuperacion_anual)))


# IDENTIFICACION CREDITOS ACTIVOS E INACTIVOS -----------------------------

### ACTIVOS

DF <- CARTERA_PD70 %>% filter(`Suma de Importe base`>0.0001) %>%
                        left_join(VALORACION_PD70,
                                  by = c('Credito'='credito')) %>%
                        rename('Saldo Actual'='Suma de Importe base') %>%
                        mutate(Precio_y_valoracion=`Precio de compra`* Valoracion_pd,
                               Precio_y_valoracion_mod= `Precio de compra`*Valoracion_pd_mod)

### CREDITOS ACTIVOS
DF_ZZ <- DF %>% filter(!is.na(valor_pago_ttl_canc))


### CREDITOS INACTIVOS
DF_X <- DF %>%filter(is.na(valor_pago_ttl_canc))


#ZZ <- CARTERA_PD70 %>% filter(`Suma de Importe base`>0.0001) %>%
#                       left_join(VALORACION_PD70,
#                           by = c('Credito'='credito')) %>%
#                      filter(!is.na(valor_pago_ttl_canc)) %>%
#                      rename('Saldo Actual'='Suma de Importe base') %>%
#                      mutate(Precio_y_valoracion=`Precio de compra`* Valoracion_pd,
#                             Precio_y_valoracion_mod= `Precio de compra`*Valoracion_pd_mod)

### INACTIVO

# X <- CARTERA_PD70 %>% filter(`Suma de Importe base`>0.0001) %>%
#                       left_join(VALORACION_PD70,
#                                 by = c('Credito'='credito')) %>%
#                       filter(is.na(valor_pago_ttl_canc)) %>%
#                       rename('Saldo Actual'='Suma de Importe base') %>%
#                       mutate(Precio_y_valoracion=`Precio de compra`* Valoracion_pd,
#                              Precio_y_valoracion_mod= `Precio de compra`*Valoracion_pd_mod)


# CREACION RESUMENES Y AJUSTE POR CREDITOS INACTIVOS ----------------------

### AGRUPACIO NCREDITOS ACTIVOS

tabla_valoracion <- DF_ZZ %>% filter(`Saldo Actual`>0.000001) %>% group_by(tipo_producto) %>% summarise(Creditos = n(),
                                             Deuda_total = sum(valor_pago_ttl_canc, na.rm = T),
                                             Saldo_a_capital = sum(`Saldo Actual`,na.rm = T),
                                             Precio_de_compra= sum(`Precio de compra`, na.rm = T),
                                             Valoracion = sum(Precio_y_valoracion,na.rm = T),
                                             Valoracion_mod = sum(Precio_y_valoracion_mod,na.rm = T)) %>%
                                  mutate(p =Valoracion/Precio_de_compra-1,
                                         p_mod = Valoracion_mod/Precio_de_compra-1)

### AGRUPACION CREDITOS INACTIVOS

residuo <- DF_X %>% filter(`Saldo Actual`>0.000001) %>% 
                select(Credito,valor_pago_ttl_canc,`Saldo Actual`,`Precio de compra`,Precio_y_valoracion)

residuo_agrupado <-  residuo %>% summarise(Creditos = n(), 
                                            Deuda_total = sum(valor_pago_ttl_canc, na.rm = T),
                                            Saldo_a_capital = sum(`Saldo Actual`,na.rm = T),
                                            Precio_de_compra= sum(`Precio de compra`, na.rm = T),
                                            Valoracion = sum(Precio_y_valoracion,na.rm = T)) %>%
                                 mutate(p =Valoracion/Precio_de_compra-1)

# DISCRIMINACION DE TABLAS POR TIPO PRODUCTO ------------------------------

### 70                  
tabla_p70 <- DF_ZZ %>% filter(tipo_producto==70) %>% select(Credito,'Saldo a capital'=`Saldo Actual`, Compra = `Precio de compra`, Precio_y_valoracion_mod) %>%
                                                  mutate(VG = Precio_y_valoracion_mod/Compra -1  ,
                                                         'Valoracion con garantia' = ifelse(is.na(VG),0,VG)) %>% select(-c(Precio_y_valoracion_mod,VG))
                                                        
### 71
tabla_p71 <- DF_X %>% filter(tipo_producto==71) %>% select(Credito,'Saldo a capital'=`Saldo Actual`, Compra = `Precio de compra`) %>%
                                                  mutate('Valoracion con garantia' = 0)


# AGRUPACION EN LISTA -----------------------------------------------------

### SE EXPORTA UN EXCEL CON VARIAS HOJAS QUE CONTIENEN LOS ELEMENTOS DE PRESENTACION: LISTA COMPLETA, TABLAS DISCRIMINADAS y RESUMENES

Lista_tablas_valoracion = list(Cartera = CARTERA_PD70,Resumen = tabla_valoracion,residuo=residuo, Vigentes_70 = tabla_p70, Vigentes_71 = tabla_p71)

# ESCRITURA COMPONENTES  --------------------------------------------------

openxlsx::write.xlsx(Lista_tablas_valoracion,'Resumen y créditos vigentes octubre 2024_modificado.xlsx')

#CC <- ZZ %>% select(Credito,`Saldo Actual`,Saldo_capital_inicial,Saldo_capital_final,saldo_capital,
#                    `Precio de compra`,tasa_recuperacion_anual,LTV,LTV_ant,valor_garantia_final,garantia_anterior)
#openxlsx::write.xlsx(CC,'ANALISIS GARANTIAS VALORACION.xlsx')
