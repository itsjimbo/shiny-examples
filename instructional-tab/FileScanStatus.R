
########################################################################
#
#  Readme
########################################################################
#  Author: James_Banasiak@bcbsil.com
#  Description: The purpose of this app is to scan a directory and visually show
#   the observered vs expected of files required to perform MRE ETL.
#  This tool is used every month to ensure that all of the files have arrived.
#  The main problem with QSIXL is their is no way to determine if all of the data had correctly come over.


# setwd("~/git/shiny-examples/instructional-tab")

#rm(list=ls())
# check for R shiny which exposes a web-interface for R..
GLOBAL_LIBS <- c("shiny", "shinyjs","DT",'futile.logger','httr','getPass','RODBC','RJSONIO','data.table','readr','stringr','stringi','devtools','xlsx','htmlTable','pander','sqldf','DBI','tcltk','base64enc','digest','dplyr','tidyr','ggplot2','ggthemes','scales','ztable','gmodels','fst')
if (length(setdiff(GLOBAL_LIBS, rownames(installed.packages()))) >  0) {
  install.packages(setdiff(GLOBAL_LIBS, rownames(installed.packages())))
}
invisible(lapply(GLOBAL_LIBS, require, character.only = TRUE))

library(shiny)
library(shinyjs)
#library('eqatools')
# from eqatools directly imported to serve as sample without installing eqatools for interview

.get.parent.func.name <- function(.where) {
  the.function <- tryCatch(deparse(sys.call(.where - 1)[[1]]),
                           error=function(e) "(shell)")
  the.function <- ifelse(
    length(grep('flog\\.',the.function)) == 0, the.function, '(shell)')

  the.function
}

checkDir=function(directory){  if (!dir.exists(directory)){ dir.create(directory) }}

appender.file2 <- function(format, console=FALSE, inherit=TRUE,
                           datetime.fmt="%Y%m%dT%H%M%S") {
  .nswhere <- -3 # get name of the function 2 deep in the call stack
  # that is, the function that has called flog.*
  .funcwhere <- -3 # ditto for the function name
  .levelwhere <- -1 # ditto for the current "level"
  function(line) {
    if (console) cat(line, sep='')
    err <- function(e) {
      stop('Illegal function call, must call from flog.trace, flog.debug, flog.info, flog.warn, flog.error, flog.fatal, etc.')
    }
    the.level <- tryCatch(get("level", envir=sys.frame(.levelwhere)),error = err)
    the.threshold <- tryCatch(get('logger',envir=sys.frame(.levelwhere)), error=err)$threshold
    if(inherit) {
      LEVELS <- c(FATAL, ERROR, WARN, INFO, DEBUG, TRACE)
      levels <- names(LEVELS[the.level <= LEVELS & LEVELS <= the.threshold])
    } else levels <- names(the.level)
    the.time <- format(Sys.time(), datetime.fmt)
    the.namespace <- flog.namespace(.nswhere)
    the.namespace <- ifelse(the.namespace == 'futile.logger', 'ROOT', the.namespace)
    the.function <- .get.parent.func.name(.funcwhere)
    the.pid <- Sys.getpid()
    filename <- gsub('~t', the.time, format, fixed=TRUE)
    filename <- gsub('~n', the.namespace, filename, fixed=TRUE)
    filename <- gsub('~f', the.function, filename, fixed=TRUE)
    filename <- gsub('~p', the.pid, filename, fixed=TRUE)
    if(length(grep('~l', filename)) > 0) {
      sapply(levels, function(level) {
        filename <- gsub('~l', level, filename, fixed=TRUE)
        cat(line, file=filename, append=TRUE, sep='')
      })
    }else cat(line, file=filename, append=TRUE, sep='')
    invisible()
  }
}

#eqatools::checkDependencies()
source("shiny/helper.R")
source("shiny/directoryInput.R")


# Global file categories
gcategories<-c('metadata','flaggedevent','rate_summary','detail','message','filename_mapping','validation_file','outputtotals')


INPUT_FOLDER<-"\\\\sambahost\\hcmdata\\EQA\\Inbound"
GENERIC_FILENAME_PATTERN1<-'eqa'
GENERIC_FILENAME_PATTERN2<-'mre'
GLOBAL_DT_OPTIONS<-list(pageLength = 500,lengthMenu = list(c(500,1000,5000, -1), list('500', '1000','5000', 'All')), paging = T)


# read the mapping file
TABLE_MAPPING<-fread('mappings/FILE_MAPPING_TO_TABLE.csv')



checkDir("logs")
flog.threshold(INFO)
flog.appender(appender.file2(paste0("logs/file-scan-status-",Sys.Date(),".log"), console=TRUE))
flog.info("Starting scan app..")

########################################################################
#
# remove file extension
#
removeExtension<-function(file){
  return(sub(pattern = "(.*)\\..*$", replacement = "\\1", file))
}


########################################################################
#
# getObservedFiles -> scans a selected directory for files with a particular category
#
getObservedFiles<-function(folder,files,category,TABLE_MAPPING){
  mappingsFound<-data.frame()
  tmpx3<- sapply(files,function(f){

    absPath<-paste0(folder,'\\',f)
    tryCatch(
      {
        file_ls <- as.character(unzip( zipfile = absPath, list = TRUE)$Name)
        i<-file_ls[[1]]

        #
        #  category<-'detail'; i<-'aap17_detail_20171101_102153_eqa_massexport_prod_2017_with_ce.txt'
        myTable<-paste0("TMP_",removeExtension(basename(i)))
        subCategory1<-stringr::str_match(string = myTable, pattern = paste0("^TMP_(.*)_",category,"*"))[2]
        subCategory2<-stringr::str_match(string = myTable, pattern = paste0("^TMP_",category,"_(.*?)_(.*)"))[2]
        subCategory<-coalesce(subCategory1,subCategory2)


        # if the category is not one of these then the subcategory is not being used..
        if (!category %in% c('metadata','outputtotals'))
        {
          subCategory<-''
        }

        dfMappingTable<-sqldf(fromTemplateString("select * from [TABLE_MAPPING] where category='[category]' and subcategory='[subCategory]'",list(
          '[category]'=category,
          '[subCategory]'=subCategory
        )
        ))


        # if (length(mappingTable)==0)
        # {
        #   warning(paste0(" [refusing to load] There is no mapping for the file ",f))
        # }
        if (nrow(dfMappingTable)>0)
        {
          flog.info(paste0(i,"   --> [",category,"] -> [",subCategory, "] -> [",dfMappingTable$finalTableName,"] "))
          actualFilename<-gsub(pattern = "/",replacement = "\\\\",x =unique(f) )
          de<-data.frame(actualFilename,i,file.size(absPath),dfMappingTable$perform_import,dfMappingTable$finalTableName,category,coalesce(subCategory1,subCategory2))
          names(de)<-c("zipfile","filename","filesize","perform_import","mapping","category","subcategory")
          # add a row to mappingsFound
          mappingsFound <<- rbind(mappingsFound, de)
        }
      },error=function(e){
        flog.error(e)

      },finally = function(){

      }

    )

  })
  return(mappingsFound)
}









########################################################################
#
# Client UI
#
ui <- fluidPage(
  useShinyjs(),
  tags$style(appCSS),
  title = "MRE Inbound data",

  tabsetPanel(

    tabPanel("Instructions",
             includeMarkdown("Readme.md")
             ),
    tabPanel("Mapping",
             tags$br(),
             tags$br(),
             fluidRow(
               column(width = 12,
                      div("Instructions:  This app helps to compare expected and observed file from Inovalon.
                          The Table Mapping is just to show how the data files will be mapped to Teradata Tables.
                          ")
               )
             ),
             tags$br(),
             tags$hr(),
             fluidRow(
               column(width = 12,
                      div("For example, below is the table mapping for how data will be loaded.  This file mappings/FILE_MAPPING_TO_TABLE.csv can be edited to load additional data.
                          ")
                      )
                      ),
             tags$hr(),

             fluidRow(
               column(width = 12,
                      DT::dataTableOutput("TABLE_MAPPING")
               )
             )
     ),
    tabPanel("Expected Files",

             mainPanel
             (
               fluidRow(
                 column(width = 12,
                        div("Select the measure file to use as what is to be expected from the data.  When a download is intiated several measures meta data files are eventually downloaded.
                            These files are in the form of invhcsc_measure_metadata_{date}_{flowchart_id}_eqa_mre_ce_a_e_eqa_mre_ce_a_e{timestamp}_pm.zip.001 and contain all of the measures used.
                             Choose BROWSE and locate this file from the recent download directory.
                            ")
                 )
               ),
               fluidRow(
                 column(
                   width = 11,

                   fileInput("selected_expected_file", "Choose MEASURE Zip/Csv File",
                             accept = c(
                               "text/csv",
                               "text/comma-separated-values,text/plain",
                               " application/zip, application/octet-stream",
                               ".zip",
                               ".zip.001",
                               ".csv")
                   )

                 )

               ),
               fluidRow(
                 column(width = 12,
                        DT::dataTableOutput("MEASURE_TABLE")
                 )
               )
             )

    ),
    tabPanel("Observed Files",
             sidebarPanel(
               width = 2,
               checkboxGroupInput("selected_categories", "Scan for:",
                                  gcategories, selected = gcategories)
             ),
             mainPanel
             (
               fluidRow(
                 column(
                   width = 11,
                   directoryInput('selected_observed_directory', label = 'Select Observed directory', value = INPUT_FOLDER)
                 ),
                 column(
                   width = 1,
                   div(style = "position:absolute;right:1em;top:20px",
                       withBusyIndicatorUI(
                         actionButton(
                           "scanButton",
                           "Scan",
                           class = "btn-primary"
                         )
                       )
                   )

                 )
               ),
               fluidRow(
                 column(width = 12,
                        DT::dataTableOutput("mappingsFound")
                 )
               )
             )

    ),

    tabPanel("Compare",

             mainPanel
             (
               fluidRow(
                 column(width = 12,
                        div("This comparison tab show the obs vs expected
                            ")
                        )
                        ),

               fluidRow(
                 column(width = 12,
                        DT::dataTableOutput("COMPARE_TABLE")
                 )
               )
                        )

             ),

    tabPanel("Queue", DT::dataTableOutput("CURRENT_QUEUE"))


  )

)



########################################################################
#
# Server
#
server <- function(input, output, session) {



  observeEvent(
    ignoreNULL = TRUE,
    eventExpr = {
      input$selected_expected_file
    },
    handlerExpr = {


      if (is.null(input$selected_expected_file)){
        return(NULL)
      }
      #localFile<-"C:\\Users\\U365042\\AppData\\Local\\Temp\\RtmpAVSitU/2279393874d12b9d67639f15/0.001"
      localZipPath<-input$selected_expected_file$datapath
      localZipPath<-gsub( x = localZipPath,pattern = "/",replacement = "\\\\\\")
      sampleSize<-100000
      cmd<-sprintf('"C:\\Program Files\\7-Zip\\7z.exe" e -so "%s" | head -n %d',localZipPath,sampleSize)
      MEASURE_TABLE<<-fread(colClasses = 'character',verbose=F,header = TRUE,strip.white=F, blank.lines.skip = FALSE, input = cmd)


      output$MEASURE_TABLE <- DT::renderDataTable({
        DT::datatable(MEASURE_TABLE, options = GLOBAL_DT_OPTIONS)
      })


    }
  )


  observeEvent(
    ignoreNULL = TRUE,
    eventExpr = {
      input$selected_observed_directory
    },
    handlerExpr = {
      if (input$selected_observed_directory > 0) {
        # condition prevents handler execution on initial app launch

        path = chooseDirectory(default = readDirectoryInput(session, 'selected_observed_directory'))
        updateDirectoryInput(session, 'selected_observed_directory', value = path)
      }
    }
  )
  output$directory = renderText({
    readDirectoryInput(session, 'selected_observed_directory')
  })


  output$TABLE_MAPPING <- DT::renderDataTable({
    DT::datatable(TABLE_MAPPING,
                  options = GLOBAL_DT_OPTIONS)

  })



  output$CURRENT_QUEUE <- DT::renderDataTable({
    DT::datatable(TABLE_MAPPING,
                  options = GLOBAL_DT_OPTIONS)
  })




  observeEvent(
    ignoreNULL = TRUE,
    eventExpr = {
      input$scanButton
    },
    handlerExpr = {


      if (length(input$selected_categories)==0 ) {
        return(NA)
      }

      tempcsv<-tempfile("tmp_",fileext=".csv")

      output$mappingsFound<- withProgress(message = 'Scanning...', value = 0, {

        selected_categories<-c(input$selected_categories)
        flog.info("%s %s",class(selected_categories),selected_categories)

        folder<-readDirectoryInput(session, 'selected_observed_directory')

        # folder<-INPUT_FOLDER;selected_categories<-'metadata'

        fileCategories<<-sapply(selected_categories,function(category){
          regex<-paste0(".*_",category,"_.*",GENERIC_FILENAME_PATTERN1,".*",GENERIC_FILENAME_PATTERN2,".*")
          flog.info(regex)
          return(list.files(folder,regex,recursive = T))
        },simplify = F)




        dfs<-lapply(seq_along(fileCategories), function(x,ns){
          category<-ns[[x]]
          incProgress(1/length(selected_categories), detail = paste(category))
          dfs<-lapply(fileCategories[category],function(files){
            # files<-fileCategories[[1]]; category=names(fileCategories)[[1]]

            df<-getObservedFiles(folder ,files,category=category, TABLE_MAPPING=TABLE_MAPPING )
            return(df)
          })
          return(do.call("rbind",dfs))
        },ns=names(fileCategories))



        df<-do.call("rbind", dfs)
        write.csv(file = tempcsv,x = df)
        #tmpdf<-fread(colClasses = 'character',verbose=F,header = TRUE,strip.white=F, blank.lines.skip = FALSE, file = "C:\Users\\U365042\\AppData\\Local\\Temp\\RtmpgPVqny\\tmp_c4025b3f2a.csv")

        row.names(df)<-NULL

        DT::renderDataTable({
          DT::datatable(df, options =
                          GLOBAL_DT_OPTIONS)
        })
      })

      flog.info("%s",tempcsv)
      # now after a scan is complete build the comparison table  of MEASURE_TABLE and df
      # this really only works on details,flaggedevents,messages. EQA at this point is not importing messages
      # the metadata and other are not needed for compare
      df<-data.table::fread(colClasses = 'character',verbose=F,header = TRUE,strip.white=F, blank.lines.skip = FALSE, file = tempcsv)

      df_measures<-sqldf("SELECT DISTINCT measure_key from MEASURE_TABLE")
      df_flaggedevent<-df[df$category=='flaggedevent',]
      df_details<-df[df$category=='detail',]
      #df_messages<-df[df$category=='messages',]



      df_no_flaggedevent<-sqldf("select 'flaggedevent' as exp_category,t1.measure_key,t2.* from [df_measures] t1 left join df_flaggedevent t2 on (t1.measure_key=t2.subcategory)")
      df_no_details<-sqldf("select 'detail' as exp_category,t1.measure_key,t2.* from [df_measures] t1 left join df_details t2 on (t1.measure_key=t2.subcategory)")
      #df_no_messages<-sqldf("select 'messages' as exp_category,t1.measure_key,t2.* from [df_measures] t1 left join df_messages t2 on (t1.measure_key=t2.subcategory)")


      output$COMPARE_TABLE <-  DT::renderDataTable({
        DT::datatable(rbind(df_no_details,df_no_flaggedevent), options = GLOBAL_DT_OPTIONS)
      })



  })

}


shinyApp(ui, server)
