#28 febbraio 2018
#File di partenza: il file csv che elenca le stazioni in base ai criteri di lunghezza e percentuale di anni disponibili
#
#Output: file di output con NOMEREGIONE_PARAMETRO_monthly.csv
#
#I file delle varie regioni vanno quindi uniti (cbind) PER CREARE UN UNICO DATA FRAME DA USARE PER LA CLUSTER ANALYSIS
rm(list=objects())
library("tidyverse")
library("magrittr")
library("purrr")
source("ClimateData.R")
source("ClimateObjects.R")
options(error=recover,warn = 2)


# DA SELEZIONARE ----------------------------------------------------------
PARAMETRO<-c("prcp","tmax","tmin")[c(2)] ##<---- SELEZIONARE UN PARAMETRO
stopifnot(length(PARAMETRO)==1)

MINLEN<-10 #<------SELEZIONARE NUMERO MINIMO DI DATI NON NA NELLE SERIE
PERCENTUALE<-95 #selezionare la percentuale di dati presenti

ANNOI<-1961 #<------SELEZIONARE ANNOI E ANNOF, rispetto a cui verrà filtrata la serie
ANNOF<-2015

###########################################################################
# INIZIO PROGRAMMA --------------------------------------------------------
###########################################################################

giornoI<-as.Date(paste0(ANNOI,"-01-01"))
giornoF<-as.Date(paste0(ANNOF,"-12-31"))
yymmdd<-as.character(seq.Date(from=giornoI,to=giornoF,by="day"))
yymm<-as.character(seq.Date(from=giornoI,to=giornoF,by="month"))

data.frame(yymmdd=yymmdd) %>% separate(yymmdd,c("yy","mm","dd"),sep="-")->calendarioDaily
data.frame(yymmdd=yymm) %>% separate(yymmdd,c("yy","mm","dd"),sep="-") %>% select(-dd)->calendarioMonthly


#nome della regione
#nome della regione, la acquisiamo dal nome della directory: si assume che la directory abbia
#nome secondo lo schema spatial_controls_NOMEREGIONE_altro
tolower(str_replace(str_replace(getwd(),"^.+controls_",""),"_.+$",""))->REGIONE

c("abruzzo","aosta","bolzano","calabria","basilicata","umbria","aeronautica","piemonte","lombardia","friuli",
  "veneto","liguria","emilia","toscana","marche","lazio","molise","campania","puglia","sicilia","sardegna")->REGIONI

stopifnot(REGIONE %in% REGIONI)

#lettura file dati
tryCatch({
list.files(pattern=paste0("^longStaz_.+",MINLEN,"\\.csv$")) %>%
  read_delim(.,delim=";",col_names=TRUE) %>% 
    filter(percentuale >= PERCENTUALE) %>%
      filter(parametro==tolower(PARAMETRO))
},error=function(e){  
  stop("Errore lettura file con elenco stazioni")
})->dfOut

if(!nrow(dfOut)) stop("L'elenco delle stazioni utili è vuoto, finisco qui!")

dfOut %>% 
  group_by(codStaz,parametro,minlen,regione,annoI,annoF) %>% 
    summarise(percentuale=max(percentuale))->stazioniSelezionate


purrr::map(stazioniSelezionate$codStaz,.f=function(codice){

    tryCatch({
      read_delim(file=paste0(codice,".txt"),delim=",",col_names=TRUE,col_types ="iiiddd") %>%
        filter(year>= ANNOI & year<= ANNOF)
    },error=function(e){
      NULL
    })->dati
  
    if(!nrow(dati)) stop("Questa stazione non può non avere dati, cmpare nell'elencio delle serie utili!")
    
    grep(PARAMETRO,names(dati))->colonna
    stopifnot(length(colonna)==1)
    
    ClimateData(x=dati[,c(1,2,3,colonna)],param=tolower(PARAMETRO))->climateDaily
    aggregaCD(climateDaily,max.na = 5,rle.check = TRUE,max.size.block.na = 3)->climateMonthly
    names(climateMonthly)<-codice

    as.data.frame(climateMonthly)
    
  }) %>% compact->listaSerieMensili
  
  #nessun dato..significa che il file con l'elenco delle stazioni utili deve essere vuoto
  if(!length(listaSerieMensili)) return(NULL)

  listaSerieMensili %>% reduce(.,left_join,by=c("yy"="yy","mm"="mm"),.init=calendarioMonthly)->monthlyMerged
  names(monthlyMerged)<-stringr::str_replace_all(names(monthlyMerged),"^X","")

  #scrittura file di output  
  write_delim(monthlyMerged,path=paste0(REGIONE,"_",tolower(PARAMETRO),"_monthly.csv"),delim=";",col_names=TRUE)
