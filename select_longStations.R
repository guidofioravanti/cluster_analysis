#28 Febbraio 2018

#File di input: i singoli ".txt" di ogni singola stazione dopo aver eseguito i controlli di qualità (anche spaziali)

#Questo programma restituisce 4 file:
# Un file statAnni, riporta per ogni controllo di qualità l'anno in cui si è verificato
#Se si è verificato in piu anni il flag, il controllo compare su più righe. Questa informazione
#può essere utile se si volesse verificare qual'è l'anno più "flaggato" per tipo di controllo.
rm(list=objects())
library("dplyr")
library("readr")
library("purrr")
library("magrittr")
source("ClimateData.R")
options(warn=2,error=recover)


# DA SELEZIONARE ----------------------------------------------------------
qualePARAMETRO<-c("prcp","tmax","tmin")[c(2,3)] ##<---- SELEZIONARE UNO o ANCHE PIU PARAMETRi
ANNOI<-1961
ANNOF<-2015
#Se FILTRARE==FALSE i controlli vengono fatti sulla serie intera, se FILTRARE==TRUE la serie
#viene inizialmente filtrata su ANNOI e ANNOF
FILTRARE<-TRUE 

# UTILITY FUNCTIONS -------------------------------------------------------
leggiDati<-purrr::partial(...f=read_delim,delim=",",col_names=TRUE,col_types ="iiiddd" )
verificaLista<-function(x){ all(purrr::map_int(x,length)==0) }

###########################################################################
# INIZIO PROGRAMMA --------------------------------------------------------
###########################################################################

#NUMERO MINIMO DI ANNI PER UNA SERIE PER ESSERE SELEZIONATA
seq(5,50,5)->NUMERO_MINIMO_ANNI

#PERCENTUALE MINIMA DI DATI
c(80,85,90,95,100)->PERCENTUALI_MINIME

list.files(pattern="^.+txt$")->nomiFile
stopifnot(length(nomiFile)!=0)

#nome della regione, la acquisiamo dal nome della directory: si assume che la directory abbia
#nome secondo lo schema spatial_controls_NOMEREGIONE_altro
tolower(str_replace(str_replace(getwd(),"^.+controls_",""),"_.+$",""))->REGIONE

c("abruzzo","aosta","bolzano","calabria","basilicata","umbria","aeronautica","piemonte","lombardia","friuli",
  "veneto","liguria","emilia","toscana","marche","lazio","molise","campania","puglia","sicilia","sardegna")->REGIONI

stopifnot(REGIONE %in% REGIONI)

purrr::map(qualePARAMETRO,.f=function(PARAMETRO){
  
      ifelse(PARAMETRO=="prcp",4,ifelse(PARAMETRO=="tmax",5,6))->COLONNA #colonna corrispondente a PARAMETRO  

      purrr::map(nomiFile,.f=function(ffile){
      
        codStaz<-str_replace(ffile,"\\.txt","")
      
        tryCatch({
          leggiDati(file=ffile)
        },error=function(e){
          stop(sprintf("Errore lettura file %s",ffile))
        })->dati
        
        dati %<>% rename(yy=year,mm=month,dd=day)
        
        if(FILTRARE){
          
          dati %<>% filter(yy>=ANNOI & yy<=ANNOF)
          if(!nrow(dati)){
            print(sprintf("FILE %s senza dati tra %s e %s",ffile,ANNOI,ANNOF))
            return(NULL)
          }
            
        }#if su FILTRARE  
          
        purrr::map(NUMERO_MINIMO_ANNI,.f=function(MINLEN){
        
              nomeParam<-names(dati)[COLONNA]
          
              dati%>%
                select(1,2,3,COLONNA) %>% 
                  ClimateData(x=.,param=nomeParam) %>%
                    aggregaCD(x=.,max.na=5,rle.check=TRUE,max.size.block.na =3) %>%
                      aggregaCD.monthly(ignore.par=FALSE,max.na=3,rle.check=FALSE,max.size.block.na=3,seasonal=TRUE)->serieAnnuale
              
              
              purrr::map(PERCENTUALI_MINIME,.f=function(percentuale){
                
                  #checkSeriesValidity2 restituisce una serie solo se il numero di dati validi (non NA) è almeno MINLEN
                  checkSeriesValidity2(x=serieAnnuale,minLen=MINLEN,percentualeAnniPresenti = percentuale,max.size.block.na=4)->ris
          
                  #se la serie non supera il controllo di continuità e completezza, ris è NULL
                  if(is.null(ris)) return(NULL)  
                  
                  #anni in cui abbiamo l'aggregato annuale
                  index(ris[!is.na(ris),])->anni
                  
                  #primo anno in cui si ha un dato valido e ultimo anno valido
                  as.integer(year(c(anni[1],anni[length(anni)])))->rangeAnniSerie 
                  
                  c(percentuale,rangeAnniSerie)->finalInfo
                  names(finalInfo)<-c("percentuale","annoI","annoF")
          
                  finalInfo
                  
              })->listaSerieSelezionate  #fine lapply su percentuale
      
              #lista fatta solo di NULL, ovvero la serie non supera mai il controllo di qualità
              if(verificaLista(listaSerieSelezionate)) return(NULL)        
              
              purrr::compact(listaSerieSelezionate) %>% reduce(rbind)->ris
      
              if(!is.matrix(ris)) ris<-data.frame(percentuale=ris["percentuale"],annoI=ris["annoI"],annoF=ris["annoF"])
      
      
              as.data.frame(ris) %>% mutate(codStaz=codStaz,regione=REGIONE,parametro=nomeParam,minlen=MINLEN)
      
      })->listaSerieMINLEN #fine lapply su MINLEN
         
      
        #nessuna serie valida
        if(verificaLista(listaSerieMINLEN)) return(NULL) 
      
        compact(listaSerieMINLEN) %>% reduce(rbind)
      
      })->listaFinale #su nomifile
      
      if(verificaLista(listaFinale)) return()
      
      #data.frame con i risultati
      reduce(listaFinale,rbind)
  
}) %>% compact->listaDaScrivere

if(verificaLista(listaDaScrivere)) stop("Nessun risultato da scrivere")  

listaDaScrivere %>% reduce(rbind)->daScrivere

lapply(NUMERO_MINIMO_ANNI,FUN=function(mm){
  
      daScrivere %>%
        filter(minlen==mm) %>%
          write_delim(.,path=paste0("longStaz_",REGIONE,"_",mm,".csv"),col_names=TRUE,delim=";")
  
})
