library("RCurl")
library("rjson")

# Esta função é adicionada para transformar os dados do Power BI em uma lista
createList <- function(dataset)
{
  temp <- apply(dataset, 1, function(x) as.vector(paste(x, sep = "")))
  colnames(temp) <- NULL
  temp <- apply(temp, 2, function(x) as.list(x))
  return(temp)
}

# Accept SSL certificates issued by public Certificate Authorities
options(RCurlOptions = list(cainfo = system.file("CurlSSL", "cacert.pem", package = "RCurl")))

h = basicTextGatherer()
hdr = basicHeaderGatherer()


req = list(

        Inputs = list(

 
            "input1" = list(
                "ColumnNames" = list("Col1", "Col2", "Col3", "Col4"),
                "Values" = createList(dataset)
            )                ),
        GlobalParameters = setNames(fromJSON('{}'), character(0))
)

body = enc2utf8(toJSON(req))
api_key = "X9Z2GmLvs0Cz2RxAOkQICZWpXyAjxDYIdiHOI5+dUG+sBI+5snqwktsFk9Yd6fBhBXJ7QSiXDMgfPtMsBFLpQQ==" # Replace this with the API key for the web service
authz_hdr = paste('Bearer', api_key, sep=' ')

h$reset()
curlPerform(url = "https://ussouthcentral.services.azureml.net/workspaces/38b9e14fe0664d5d9e654cdeb75de6cf/services/dd8b91b8485747a9a9f6b6a77af7d4fa/execute?api-version=2.0&details=true",
            httpheader=c('Content-Type' = "application/json", 'Authorization' = authz_hdr),
            postfields=body,
            writefunction = h$update,
            headerfunction = hdr$update,
            verbose = TRUE
)

headers = hdr$value()
httpStatus = headers["status"]
if (httpStatus >= 400)
{
  print(paste("The request failed with status code:", httpStatus, sep=" "))
  
  # Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
  print(headers)
}

result = h$value()
finalResult <- fromJSON(result)
# inter <- do.call("rbind", finalResult$Results$output1$value$Values)
# MyFinalResults <- data.frame(inter)
# names(MyFinalResults) <- finalResult$Results$output1$value$ColumnNames
# rm(list=setdiff(ls(), "MyFinalResults"))