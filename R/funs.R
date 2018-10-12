# library(dplyr)
# library(httr)
# library(magrittr)
# library(xml2)
# library(rvest)
# library(readr)
# library(janitor)
# library(lubridate)
# library(readODS)
# library(jsonlite)

# url_raw <- "http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/"
# output_file <- "informe_diario.csv"
# 
# csv_links <- httr::GET(url_raw) %>% 
#   httr::content("text") %>% 
#   #rvest::html_nodes("td")
#   #xml_find_all(xpath = ".//*[name()='loc']")
#   xml2::read_html() %>% 
#   rvest::html_nodes("a") %>% 
#   rvest::html_attr("href") #%>% stringr::str_subset("csv")
# 
# csv_links <- csv_links[which(grepl(x = csv_links, pattern = "*.csv"))]
# ultimo_csv <- csv_links[length(csv_links)]
# 
# download_link <- paste0(url_raw, ultimo_csv)
# 
# download.file(download_link, destfile = output_file,
#               method = "curl")

# help functions ####
parse_num_ods_col <- function(x){
  parsed <- gsub('[R$]', '', x, fixed = FALSE)
  parsed <- gsub('.', '', parsed, fixed = TRUE)
  parsed <- gsub(',', '.', parsed, fixed = TRUE)
  # in case of parenthesis, replace by a negative sign
  # replace_parenthesis <- function(x){
  #   x <- gsub("[()]", "", a)
  #   paste()
  # }
  parsed <- if_else(grepl("[()]", parsed),
                   paste0("-", gsub("[()]", "", parsed)),
                   parsed)
  
  as.numeric(parsed)
}


download_cvm_file <- function(csv_url, download_path = NULL, verbose = TRUE){
  
  if (is.null(download_path)){
    download_path <- tempfile()
  }
  
  if (verbose){
    message("Downloading file")
  }
  
  download.file(url = csv_url, destfile = download_path, method = "curl")
  
  download_path
}

read_cvm_file <- function(file, sheet = 1, skip = 5, verbose = TRUE){
  if (verbose){
    message("Reading file")
  }
  suppressMessages({
    readODS::read_ods(path = file, sheet = sheet, skip = skip)
  })
}

# API FUNDOS ####


# importar arquivo de informe diario
#ultimo_inf_diario <- "informe_diario.csv"

#df <- read.csv2(ultimo_inf_diario, stringsAsFactors = FALSE)


# Retorna quota de fundos de investimento
#
# Através do cnpj, etorna a quota de fundos de investimento a partir do último informe diário da CVM
# 
# @param cnpj_busca cpnj da do fundo
#
# @export
# retornar_quota <- function(cnpj_busca){
#   cota <- as.numeric(df$VL_QUOTA[df$CNPJ_FUNDO == cnpj_busca])
#   cota <- cota[length(cota)]
#   cota
# }


#' Retorna quota de fundos de investimento
#'
#' Através do cnpj, retorna a quota de fundos de investimento a partir do último informe diário da CVM
#' 
#' @param CNPJ_BUSCA cpnj da do fundo
#'
#' @export
retornar_quota_recente <- function(CNPJ_BUSCA){
  # definir url base
  url_base <- "http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/"
  
  # extrair todos os csvs na pagina
  csv_links <- httr::GET(url_base) %>% 
    httr::content("text") %>% 
    xml2::read_html() %>% 
    rvest::html_nodes("a") %>% 
    rvest::html_attr("href") 
  
  csv_links <- csv_links[which(grepl(x = csv_links, pattern = "*.csv"))]
  # extrair ultimo csv
  ultimo_csv <- csv_links[length(csv_links)]
  # importar csv para o R
  ultimo_csv <- paste0(url_base, ultimo_csv)
  suppressMessages({
    df <- as.data.frame(
      readr::read_delim(ultimo_csv,
                        delim = ";",
                        locale = readr::locale(decimal_mark = ".",
                                               grouping_mark = ""))
    )
  })
  
  df_fundo_input <- subset(df, CNPJ_FUNDO == CNPJ_BUSCA)
  # selecionar ultima linha
  df_fundo_input <- df_fundo_input[nrow(df_fundo_input), ]
  # retornar string com data de referencia e valor da quota
  data <- as.character(df_fundo_input[, "DT_COMPTC"])
  quota <- df_fundo_input[, "VL_QUOTA"]
  output <- data.frame(DATA_QUOTA = data,
                       QUOTA = quota,
                       stringsAsFactors = FALSE)
  return(jsonlite::toJSON(output))
}


#http://dados.cvm.gov.br/dados/FI/CAD/DADOS/inf_cadastral_fi_20180906.csv

#' Retorna um csv com informações sobre os fundos de investimento
#'
#' Retorna o csv mais atual contendo informações sobre os fundos de investimento registrados na CVM
#'
#' @export
download_cadastro_fundos <- function(download_path = NULL, verbose = TRUE){
  # do web scraping to get the last filename
  base_url <- "http://dados.cvm.gov.br/dados/FI/CAD/DADOS/"
  table <- rvest::html_table(xml2::read_html(base_url))[[1]]
  filename <- table[nrow(table)-1, 2]
  
  url_cadastro <- paste0("http://dados.cvm.gov.br/dados/FI/CAD/DADOS/", filename)
  # save downloaded file on a temporary file
  file <- download_cvm_file(csv_url = url_cadastro, download_path, verbose)
  # read the downloaded file
  readr::read_delim(file,
                    delim = ";", 
                    locale = locale(encoding = "ISO-8859-1"))
}


# Emissores ####



#' Retorna uma tabela contendo o informe diário
#'
#' Retorna a tabela mais atual contendo o informe diário com informações agregadas sobre valor total de captação, valor total dos resgates, quantidade de cotistas, total da carteira e patrimônio líquido
#'
#' @export
emissores_informe_diario <- function(download_path = NULL, verbose = TRUE){
  # save downloaded file on a temporary file
  csv_url <- "http://www.cvm.gov.br/menu/acesso_informacao/serieshistoricas/serieshistoricas/anexos/Informe_diario__Planilha_Agregada__Fundos_555__Total.ods"
  file <- download_cvm_file(csv_url, download_path, verbose)
  
  ## read downloaded file
  x <- read_cvm_file(file, verbose = verbose) 
  # remove invalid columns
  x[, 1:2] <- NULL
  # clean colnames
  x <- janitor::clean_names(x)
  # rename date column
  colnames(x)[1] <- "data"
  
  ## convert column types
  # date column
  x[,1] <- lubridate::dmy(x[,1])
  
  
  for (i in 2:ncol(x)){
    x[, i] <- parse_num_ods_col(x[, i])
  }
  
  x
}


#' Retorna uma tabela com informações sobre os ativos dos fundos de investimento
#'
#' Retorna uma tabela contendo as mais atuais informações sobre os ativos de todos os fundos de investimento registrados na CVM
#'
#' @export
emissores_ativos_em_carteira <- function(download_path = NULL, verbose = TRUE){
  csv_url <- "http://www.cvm.gov.br/menu/acesso_informacao/serieshistoricas/serieshistoricas/anexos/Ativos_em_Carteira__Fundos_555__Total.ods"
  file <- download_cvm_file(csv_url, download_path, verbose)
  ## read downloaded file
  x <- read_cvm_file(file, verbose = verbose) 
  # clean names
  x <- janitor::clean_names(x)
  # remove first col
  x[, 1] <- NULL
  
  # for date columns, replace month names by number
  colnames(x)[1] <- "data"
  x$data <- gsub("jan", "01", x$data)
  x$data <- gsub("fev", "02", x$data)
  x$data <- gsub("mar", "03", x$data)
  x$data <- gsub("abr", "04", x$data)
  x$data <- gsub("mai", "05", x$data)
  x$data <- gsub("jun", "06", x$data)
  x$data <- gsub("jul", "07", x$data)
  x$data <- gsub("ago", "08", x$data)
  x$data <- gsub("set", "09", x$data)
  x$data <- gsub("out", "10", x$data)
  x$data <- gsub("nov", "11", x$data)
  x$data <- gsub("dez", "12", x$data)
  
  # convert to Date type
  x$data <- as.Date(ISOdate(year = paste0("20", substr(x$data, 4, 5)), 
                            month = substr(x$data, 1, 2),
                            day = 1))
  
  # numeric columns
  for (i in 2:ncol(x)){
    x[, i] <- parse_num_ods_col(x[, i])
  }
  
  x
}

# Caracteristicas dos fundos ####

#' Retorna uma tabela com informações sobre as características dos fundos
#'
#' Retorna uma tabela contendo as mais atuais informações sobre as características de todos os fundos de investimento registrados na CVM
#'
#' @export
emissores_caracteristicas_fundos <- function(download_path = NULL, verbose = TRUE){
  csv_url <- "http://www.cvm.gov.br/menu/acesso_informacao/serieshistoricas/serieshistoricas/anexos/Caracteristicas_dos_Fundos__Planilha_Agregada__Fundos_555__Total.ods"
  file <- download_cvm_file(csv_url, download_path, verbose)
  ## read downloaded file
  x <- read_cvm_file(file, verbose = verbose) 
  # clean names
  x <- janitor::clean_names(x)
  
  total_fundos_mes <- rowSums(x[, 3:4])
  
  x_final <- cbind(
    # vetor de datas
    data = x[, 2],
    total_fundos = total_fundos_mes,
    # proporcao de fics
    fic_sim = x[, 4]/total_fundos_mes,
    # condominio
    condominio_aberto = x[, 9]/total_fundos_mes,
    # exclusivo, sim ou nao
    exclusivo_sim = x[, 13]/total_fundos_mes,
    # qualificado, sim ou nao
    qualificado_sim = x[, 17]/total_fundos_mes,
    # classe dos fundos
    acoes = x[, 25]/total_fundos_mes,
    renda_fixa = x[, 26]/total_fundos_mes,
    multimercado = x[, 27]/total_fundos_mes,
    cambial = x[, 28]/total_fundos_mes
  )
  
  x_final <- as.data.frame(x_final)
  # convert date column
  x_final$data <- as.Date(ISOdate(year = as.numeric(substr(x_final$data, 1, 4)),
                                  month = as.numeric(substr(x_final$data, 5, 6)),
                                  day  = 1))
  x_final
  
}




# adiciona dependencias de pacotes externos
# devtools::use_package('dplyr', pkg = 'apiFundo')
# devtools::use_package('httr', pkg = 'apiFundo')
# devtools::use_package('magrittr', pkg = 'apiFundo')
# devtools::use_package('xml2', pkg = 'apiFundo')
# devtools::use_package('rvest', pkg = 'apiFundo')
# devtools::use_package('readr', pkg = 'apiFundo')
# devtools::use_package('janitor', pkg = 'apiFundo')
# devtools::use_package('lubridate', pkg = 'apiFundo')
# devtools::use_package('readODS', pkg = 'apiFundo')
# devtools::use_package('jsonlite', pkg = 'apiFundo')
# abjutils::use_pipe()

# documenta o pacote
#devtools::document('apiFundo')

# instala o pacote na máquina
#devtools::install('apiFundo')
# d






