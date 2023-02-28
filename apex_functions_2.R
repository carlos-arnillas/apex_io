require(data.table)

# Two columns that need to be standardized
apex_r_fields <- c("crop_col_id", "file")

# Generate master variables 
folders <- list(apex="../apex_files/apex_in/",
                #apex="../apex0/Scenarios/Default/TxtInOut/",
                dem="../apex_files/dem/",
                general="../apex_files/general/",
                hydro="../apex_files/hydro/",
                lu="../apex_files/lu/",
                soils="../apex_files/soils/",
                weather="../apex_files/weather/",
                zones="../apex_files/zones/")

files <- list(mask=paste0(folders$zones, "mask.shp"),
              delineation=paste0(folders$lu, "delineation.shp"),
              channels=paste0(folders$hydro, "channels.shp"),
              dem=paste0(folders$dem, "dem.tif"),
              apex_app="../../../../../model/APEX/src/apex_utsc")

db_str_fn <- "../../apex_r/codes/database_structure2.xlsx"
lu_lk_fn <- "../../apex_r/codes/landuse_link.xlsx"

# This is a general variable that should force everything to be redone.
if (!exists("redo")) redo <- FALSE

# Get the list of files 
zip_input_files <- function(zfn, folder=NULL, ...) {
  # Get the structure
  d1 <- load_apex_file_str()
  # Get the input file names
  if (is.null(folder)) {
    folder <- if (exists("folders")) folders$apex else "."
  } else {
    folder <- folder[1]
  }
  # find the files
  lf <- dir(folder, paste0(gsub("^[xy]\\.", "^.*\\.", unique(d1$apex_fn)), collapse="|"), 
            full.names=TRUE)
  res <- zip(zfn, lf, ...)
  if (res != 0) {
    message(sprintf("Error compressing the files. (#%d)", res))
  }
}

# Create run_apex.bat / run_apex.sh
write_run_apex <- function(afol, dbg=FALSE, apex_prog=NULL) {
  syst <- switch(toupper(substr(getwd(),1,1)), 
                 `/`=,`~`="linux",                              # we are in a linux server
                 E=, D=, C="windows")
  if (is.null(apex_prog)) {
    apex_prog <- switch(syst, 
                     linux="~/apex/apex_utsc",                              # we are in a linux server
                     windows=sprintf("C:/APEX/APEX_latest_LD/apex1501_%s.exe", ifelse(dbg[1], "Debug", "Release"))) # we are in windows
  }
  afol <- normalizePath(afol, "/")
  atxt <- switch(syst, 
                 windows=gsub("^([A-Z]):(.*)$", "\\1:\ncd \\2", afol),
                 linux  =c("#!/bin/bash",                                # Bash-file firs line
                           sprintf("cd %s", gsub(" ", "\\\\ ", afol))))     # Change folder
  file_to <- paste0(afol, "/run_apex.", switch(syst, windows="bat", linux="sh"))
  post_proc <- switch(syst, windows="", linux='for d in " "*; do mv -f "$d" "${d// /}"; done')
  writeLines(c(atxt, apex_prog, post_proc), file_to)
}


# Obtained from raster 3.4-13
unique_names <- function (x, sep = ".") {
  dups <- unique(x[duplicated(x)])
  for (dup in dups) {
    j <- which(x == dup)
    x[j] <- paste(x[j], sep, 1:length(j), sep = "")
  }
  x
}

# Read the values in a given file, line, column. Values can be repeated every reps lines
apex_read <- function(fn,ln,col,reps, folder=NULL) {
  # Setting the folder
  if (is.null(folder)) {
    folder <- if (exists("folders")) folders$apex else "."
  } else {
    folder <- folder[1]
  }
  ls <- unique(c(length(fn), length(ln), length(col), 1))
  if (length(setdiff(ls, c(1, length(fn)))) > 0) {
    stop("Lengths don't match.")
  }
  ls0 <- data.table(fn, ln, col)
  ls0[,id:=1:nrow(ls0)]
  #browser()
  values <- ls0[,by=.(id,fn,ln0=as.integer(ln),reps,col),{
    # Read the file
    lf <- readLines(paste0(folder, fn))
    # Remove empty rows at the end (there could be a better way...)
    # browser()
    cnt <- length(lf); repeat {if (nchar(lf[cnt])==0) cnt <- cnt-1 else break}
    # transform the file into a table
    lf <- strsplit(trimws(lf), " +")
    v1 <- if (is.na(reps)) {
      ln0 
    } else {
      # Numbering the lines
      ix <- 1:length(lf) %% reps
      ix[ix==0] <- reps
      # and finding the ones that match
      which(ix == ln0)
    }
    res <- data.table(ln=v1, sapply(lf[v1], function(x) x[col]))
    setnames(res, c("ln", "value0"))
    # removing values after the last non-empty row
    res[ln <= cnt]
  }]
  #browser()
  # Organizing the data
  values[, id := NULL]
  #print(values)
  return(values)
}

# This function changes one value at a time
substr_col0 <- function(tx, nv, ln, col, length_check=TRUE) {
  #browser()
  if (length(nv) > 1) browser()#stop("new value has to be a length 1 character vector")
  if (is.na(nv)) {
    warning("No NA values can be added.")
    return(tx[ln])
  }
  # Store the text structure
  chpnt <- nchar(tx)
  # Estimate the breakpoints
  ptx <- c(0, gregexpr("[0-9.][[:space:]]",tx[ln])[[1]], nchar(tx[ln]))
  # length of the "column"
  lnr <- ptx[col+1] - ptx[col]
  # first position of the column
  strt <- ptx[col] + 1
  # Same length?
  if (lnr < nchar(nv)) stop("Replacement string longer than original one.")
  # Replacing
  substr(tx[ln], strt, strt+lnr-1) <- formatC(nv, width=lnr)
  # Everything ok?
  if (any(nchar(tx)!=chpnt) & length_check) stop("Something changed the text length.")
  return(tx[ln])
}

# This function will change the values in several lines at the same file
substr_col <- function(tx, nv, ln, col, length_check=TRUE) {
  if (unique(length(nv), length(col))!=length(ln)) stop("Not all parameters have the same lengths.")
  tx0 <- tx
  for (i in 1:length(nv)) {
    tx0[ln[i]] <- substr_col0(tx0[ln[i]], nv[i], 1, col[i], length_check=length_check)
  }
  return(tx0)
}

# Read FWF format with autodetection of column widths
# The column widths are defined using the first row with data, not with column names
# If skip begins with a question mark (e.g., '?25'), it will copy the first rows to the clipboard (e.g., 25)
read_fwf_ad <- function(x, skip=0, fixed=TRUE, col.names="..SEARCH..") {
  # read the file
  cat("Reading ", x, "\n")
  txt <- readLines(x)
  cx <- character()
  colClasses <- NA
  flip <- FALSE
  # drop the first lines if needed
  if (is.character(skip)) {
    if (grepl("\\?[0-9]*$", skip)) {
      skip <- substr(skip, 2,1000)
      nl <- if (skip != "") as.integer(skip) else 20
      if (.Platform$OS.type=="unix") {
        writeLines(txt[1:nl], pipe("pbcopy"))
      } else {
        writeLines(txt[1:nl], "clipboard")
      }
      message(sprintf("\nFirst %d lines (out of %d) copied to the clipboard", nl, length(txt)))
      return(data.table(txt=txt[1:nl]))
    }
    skip <- grep(skip, txt, fixed=fixed)
    skip <- if (length(skip)==0) 0 else skip[1] - 1
  }
  if (skip > 0) txt <- txt[-(1:skip)]
  # Counting characters
  nc_txt <- nchar(txt) 
  
  # Estimate the breakpoints using the longest row with data
  ptx <- c(0, gregexpr("[[:alnum:]#][[:space:]]",txt[which.max(nc_txt)])[[1]], 
           nchar(txt[which.max(nc_txt)]))
  # get the width using the column names
  widths <- ptx[-1] - ptx[-length(ptx)]
  # clean memory and read the file
  # rm(txt)
  # First, read the header
  # browser()
  if (is.null(col.names)) {
    col.names <- paste0("V", seq_along(widths))
    skip <- skip-1
  } else {
    if (col.names=="..SEARCH..") {
      col.names <- strsplit(txt[1], "[[:space:]]+")[[1]] #read.fwf(x, widths=widths, skip=skip, n=1)
      col.names <- col.names[col.names != ""]
    }
    # Dealing with MSA and alike (empty column names for the columns with variable names, and months as column names)
    if (length(col.names) != length(widths)) {
      if (all(c("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", 
                "SEP", "OCT", "NOV", "DEC") %in% col.names)) {
        # If we have months as columns, look for the empty spots that should have the column names
        col.names <- trimws(mapply(function(x,y) substr(txt[1], x, y), ptx[-length(ptx)]+1, ptx[-1]))
        # Trick needed because more than one column can occur. Only the first one will be kept
        cx <- "Var"
        if (sum(col.names=="")>1) cx <- c(cx, rep("Var_1", sum(col.names=="")-1))
        col.names[col.names==""] <- cx
        # This has to be done later
        flip <- TRUE
        colClasses <- rep(NA, length(widths))
        # First, checking that the last two columns should be dropped
        if (sum(col.names=="YR")==2) {
          colClasses[which(col.names=="YR")[2]] <- "NULL"
          message(basename(x), ": Two YR columns, skipping the second one.")
          if (any(grepl("\\.MSA$", x))) message("In .MSA, the second year column is an average, but the column is il-formated, so dropping it anyway.")
        }
        if (sum(col.names=="Var_1")>0) {
          colClasses[which(col.names=="Var_1")] <- "NULL"
          message(basename(x), ": More than one empty column name, probably with variable names, Using only the first.")
        }
      }
    }
  }
  
  
  # Checking no problems in headers
  if (any(sum(widths)!=nc_txt)) {
    l1 <- which(sum(widths)!=nc_txt)
    warning(sprintf("Problem importing %s:\n  %d line(s) do not match the total width (e.g, %s).\n", 
                    x, length(l1), paste0(l1[1:min(10, length(l1))] + skip, collapse=", ")))
  }
  if (length(col.names) != length(widths)) {
    warning(sprintf("Problem importing %s.\nNumber of columns predefined or automatically identifed doesn't match.", x))
    if (length(col.names) > 0) col.names <- col.names[seq_along(widths)]
    if (any(is.na(col.names))) col.names[is.na(col.names)] <- paste0("Var_", 1:sum(is.na(col.names)))
  }
  # Now, read the table, using the headers
  res <- setDT(read.fwf(x, widths=widths, skip=skip+1, col.names=col.names, colClasses=colClasses))
  # transpose the table?
  if (flip) {
    setnames(res, c("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG",
                    "SEP", "OCT", "NOV", "DEC"), sprintf("%02d", 1:12))
    res1 <- melt.data.table(res, measure.vars=sprintf("%02d", 1:12),
                            variable.factor = FALSE, variable.name = "MONTH")
    res1[, Var := trimws(Var)]
    # Removing empty rows
    if ("" %in% res1$Var) if (res1[Var=="",sum(value)]==0) res1 <- res1[Var!=""]
    res <- dcast(res1, ... ~ Var, value.var="value")
    res[,MONTH := as.integer(MONTH)]
  }
  # browser()
  return(res)
}


# Function to select sets of variables predefined in the database_structure$APEX_Output sheet
# allowing them to be printed in a consistent way.
# vars  : list of variables to print (text codes according table 2.9)
# files : list of files
# fn    : file's name to be used when creating the print file
# crit  : criterion used to select variables from APEX_Output table. E.g., 'Balance=="W"'
write_print <- function(vars=NULL, files=NULL, fn=NULL, crit=NULL) {
  apex_out_str <- setDT(readxl::read_excel(db_str_fn, "APEX_Output"))
  # Building list of variables
  if (is.character(vars)) {
    vars0 <- data.table(Variable=vars)
    vars0[apex_out_str[!is.na(var_number)], on="Variable", var_number := var_number]
    if (any(is.na(vars0$var_number))) {
      message("No code to print the following variables (see table 2.9): ", paste0(vars0[is.na(var_number), Variable], collapse=", "))
    }
    vars <-  vars0$var_number
    rm(vars0)
  }
  # Checking the criteria
  if (!is.null(crit)) {
    ln <- apex_out_str[eval(parse(text=crit))]
    vars <- c(vars, ln[!is.na(var_number), unique(var_code)])
  } else {
    ln <- if (is.numeric(vars)) apex_out_str[var_number %in% vars] else apex_out_str[var_code %in% vars]
  }
  if (length(vars) > 40) stop("More than 40 variables requested")
  # Building list of files
  # 1. files from variables (should include data from crit)
  # 2. files from files list
  apex_out_str_fl <- read_out_files_format()
  # Which files are needed?
  files0 <- ln[,by=var_code, {if (nrow(.SD[!is.na(var_number)]))
               NULL else unique(.SD[!is.na(Extension), .(Extension)])}]
  files0 <- if (nrow(files0)) files0[apex_out_str_fl, on="Extension", PrintID] else NULL
  
  # Getting file IDs
  if (!is.null(files) & is.character(files)) {
    files <- apex_out_str_fl[Extension %in% files, PrintID]
  }
  files <- c(files, files0)
  
  # Checking file name
  if (is.null(fn)) fn <- paste0(folders$apex, "PRNT1501.DAT")
  # Preparing the file
  vars <- sprintf("%4d", vars)
  txt <- list(KA = c(vars, rep("    ", 100-length(vars))),
              JC = sprintf("%4d", c(38,39,40,49)),
              KS = sprintf("%4d", 1:20),
              KD = c(vars, rep("    ", 40-length(vars))),
              KY = c(vars, rep("    ", 40-length(vars))),
              KFL = sprintf("%4d", (1:49 %in% files)+0))
  
  # formatting
  txt <- lapply(txt, function(x) sapply(0:floor((length(x)-1)/20), 
                                        function(y) {
                                          # print(1:20 + (y)*20);
                                          # print(seq_along(y));
                                          # print(intersect(1:20 + (y)*20, seq_along(x)));
                                          paste0(x[intersect(1:20 + (y)*20, 
                                                                       seq_along(x))], 
                                                           collapse="")}))
  writeLines(unlist(txt), fn)
  invisible(TRUE)
}

# Returns the list of lines in each file that has an specific pattern 
grep_files <- function(pattern, folder=NULL, file_pattern = ".*") {
  # Setting the folder
  if (is.null(folder)) {
    folder <- if (exists("folders")) folders$apex else "."
  } else {
    folder <- folder[1]
  }
  lf <- dir(folder, pattern = file_pattern)
  rbindlist(lapply(lf, function(x) {
    rl <- readLines(paste0(folder, x))
    lines <- grep(pattern, rl)
    if (length(lines) > 0) {
      res <- data.table(file=x, lines=lines, nlines=length(rl))
    } else {
      res <- data.table(file=character(0), lines=integer(0), nlines=integer(0))
    }
  }))
}

# The function assumes that lower case, [-/.#] and numbers after the lower case are units
remove_units <- function(x) {
  gsub("(-|[.a-z(/][-a-z0-9/.#%)]*)$", "", x)
}

read_column_names <- function() {
  r <- setDT(readxl::read_excel(db_str_fn, "APEX_Output"))
  if (!("var_code" %in% names(r))) {
    message("In APEX, variable names are not unique!!! Generate var_code with unique names.")
    browser()
    r[,var_code := remove_units(Variable)]
    write.table(r[var_code != Variable,.(var_code, Variable)], "clipboard", sep="\t", row.names=F)
    write.table(r[,by=.(var_code),.(files=paste0(Extension, ": ", Variable, " - ", Description, collapse="\n"))], "clipboard", sep="\t", row.names=F)
    write.table(r[,by=.(var_code),.(files=paste0(unique(Variable), collapse=", "))], "clipboard", sep="\t", row.names=F)
  }
  return(r)
}

read_out_files_format <- function() {
  setDT(readxl::read_excel(db_str_fn, "APEX_Output_files"))
}

# Check that columns match pre-existing definitions
# - Compare with pre-existing table with names/units
# - Report any column that doesn't match & and a flag indicating the
#   general structure of the table: 
#     1: Columns are variables
#     2: Columns are months
#     3: Columns are numbers (e.g., layers)
check_column_names <- function(names, file_name=NULL) {
  # Organize the information
  res0 <- data.table(pos=1:length(names), names)
  res0[,by=names,dup := 1:.N]
  #res[,final := names]
  # Looking for Valid names
  cn <- read_column_names()
  # Including units in those variables generated as var numbers
  cn <- rbind(cn[,.(var_number, Extension, Variable, var_code)], 
              cn[!is.na(var_number), .(var_number, Extension, Variable=paste0(Variable, units), var_code)])
  # Are we checking for a specific file?
  if (!is.null(file_name)) {
    if (nrow(cn[(sub("^.*(\\.[[:alnum:]]+)$", "\\1", file_name)==Extension) | file_name == Extension])) {
      # Add all the ones that could apply
      cn <- cn[is.na(Extension) | (sub("^.*(\\.[[:alnum:]]+)$", "\\1", file_name)==Extension) | file_name == Extension]
      # If any of the options correspond to file_name, use that one
      cn <- cn[,by=Variable, {
        if (any(!is.na(Extension)) & any(Extension == file_name)) {
          .SD[Extension == file_name]
        } else {
          .SD
        }
      }]
    }
  }
  res1 <- unique(cn[,.(Variable,var_code)])[res0, on=c(Variable="names")]
  # second try, checking names are valid column names
  cn2 <- cn[Variable %like% "[/#]",.(Variable=make.names(Variable),var_code)]
  res1[cn2, on="Variable", var_code := ifelse(is.na(var_code), i.var_code, var_code)]
  # Maybe, the Variable is already the var_code
  res1[is.na(var_code) & (Variable %in% cn$var_code), var_code := Variable]
  # If variable exists, add it to final
  res1[, exists := !is.na(var_code)]
  res1[, final := ifelse(exists, var_code, Variable)]
  
  # are there months?
  res1[, months := final %in% c("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", 
                                "SEP", "OCT", "NOV", "DEC")]
  
  # are there numbers?
  res1[, numbers := grepl("^[0-9]+$", Variable)]
  
  
  # Updating names for duplicated variables
  res1[Variable %in% res1[dup > 1,Variable], final := paste0(final,"_", dup)]
  
  # organizing the information
  res <- res1[,keyby=.(pos,initial=Variable, exists, months, numbers), .(final=paste0(final,collapse="|"))]
  if (nrow(res[final %like% "\\|"])) {
    message("More than one potential variable found!\n")
    print(res[final %like% "\\|"])
  }
  
  # Forcing file to be an existing variable
  res[initial %in% apex_r_fields, exists := TRUE]
  if (any(res$months)) message("Months as columns\n")
  if (any(res$numbers)) message("Numbers as columns\n")
  return(res)
}

# Read parameter values, to assign them as labels when needed
read_parameter_values <- function() {
  return(setDT(readxl::read_xlsx(db_str_fn, sheet="parameter_values")))
}


read_param_file <- function(fn=NULL, folder=NULL) {
  # Setting the folder
  if (is.null(folder)) {
    folder <- if (exists("folders")) folders$apex else "."
  } else {
    folder <- folder[1]
  }
  # And file name
  if (is.null(fn)) fn <- "PARM1501.DAT"
  txt1 <- read.fortran(paste0(folder, fn), "10F8.")
  nc <- apply(txt1, 1, function(x) sum(!is.na(x)))
  scrp <- cbind(1:(sum(cumsum(nc!=2)==0)), as.data.table(txt1[cumsum(nc!=2)==0,1:2]))
  setnames(scrp, c("row", "SP1", "SP2"))
  parm <- rbindlist(apply(txt1[cumsum(nc!=2)>0,], 1, function(x) data.table(pos=1:length(x), value=as.numeric(x))),
                    idcol="row")
  # scrp[,row := as.integer(row)]
  parm[,row := as.integer(row)]
  parm[,id := (row - max(scrp$row) -1)*10 + pos]
  message("Importing ", nrow(scrp), " S-curve parameters and ", nrow(parm), " parameters.")
  return(list(scrp=scrp, parm=parm))
}


# Load the apex structure
load_apex_file_str <- function() {
  # Read the table and remove rows that are meaningless
  x <- setDT(readxl::read_xlsx(db_str_fn, sheet="arc_apex_tbldef", na=c("", "NA")))[is.na(ColMax) | ColMax != 0]
  # Do we have gaps in Parameter names that should be replaced with Parameter2
  if (nrow(x[is.na(Parameter)])) {
    if (nrow(x[is.na(Parameter) & is.na(Parameter2) & !(is.na(Description) & is.na(Def))])) {
      warning("Parameter and Parameter2 have empty values in ", 
              nrow(x[is.na(Parameter) & is.na(Parameter2)]), " rows.")
    }
    message("Using Parameter2 for '", paste0(x[is.na(Parameter) & !is.na(Parameter2), Parameter2], collapse="', '"), "'.")
    x[is.na(Parameter), Parameter := Parameter2]
  }
  # Add the column position for each field, and estimate the maximum number of rows
  x[order(apex_fn, Line, ColMin), by=.(apex_fn, Line), ColPos := 1:.N]
  x[, by=.(apex_fn), NumRows := max(Line)]
  return(x)
}

# Function to recreate input format for fortran
formatFrtn <- function(x, width=6, max_dec=3, type=NULL, force=FALSE) {
  # Numeric or something else?
  if (is.null(type)) type <- class(x)
  if (type == "numeric") {
    # First try
    y <- formatC(round(x, max_dec), format="f", digits=max_dec, width=width)
    # Remove trailing zeros
    y <- gsub("0+$", "", y)
    # The next part is to remove decimal places if needed
    nc <- nchar(y)
    problems <- nc - width
    # Reduce the number of decimal places when needed
    if (any(problems > 0)) y[problems > 0] <- formatC(round(x[problems > 0], 
                                                            pmax(0, max_dec-problems[problems > 0])), 
                                                      format="f")
    # Remove trailing zeros again
    y <- gsub("0+$", "", y)
    # Add a dot if none is present
    y[!grep(".", y, fixed=TRUE)] <- paste0(y[!grep(".", y, fixed=TRUE)], ".")
  } else {
    if (type == "integer") y <- as.character(as.integer(x))
  }
  # checking
  y <- sprintf(paste0("%", width, "s"), y)
  nc <- nchar(y)
  if (any(nc > width)) {
    warning("Some values are larger than allowed by width.")
    if (force) y[nc > width] <- NA
  }
  return(y)
}

# Using the apex structure, read the original values
# lvars can, in principle, be vector or a single value, the later will be taken as a reg. exp.)
# If nv is not null, it will adjust the parameter after reading it. Only works when lvars has one element only
load_in_params <- function(lvars, multi=TRUE, transpose=multi, folder=NULL, nv=NULL) {
  # Setting the folder
  if (is.null(folder)) {
    folder <- if (exists("folders")) folders$apex else "."
  } else {
    folder <- folder[1]
  }
  if (lvars %like% "\\<scrp\\>") {
    apex_fn <- "PARM1501.DAT"
    # Get the information
    scrp <- read_param_file(folder = folder)$scrp
    nm <- gsub("^.*\\<([0-9]+)\\>.*$", "\\1", lvars)
    sp0 <- gsub("^.*\\<(SP[12])\\>.*$", "\\1", lvars)
    if (!sp0 %in% c("SP1", "SP2") & !is.null(nv)) {
      message("Undefined column of S-curve parameters, impossible to adjust parameter.")
      sp0 <- NA_character_
    }
    vars0 <- data.table(fn=apex_fn, row=as.integer(nm), sp=sp0, value=scrp[row==nm][[sp0]])
    # adjust the information?
    if (!is.null(nv)) {
      if (nv < 0) warning("S-Curve values must be positive.")
      txt_f <- vars0[,readLines(paste0(folders$apex, apex_fn))]
      for (i in 1:nrow(vars0)) {
        # using 2 decimal places and only 7 out of 8 spaces to keep an empty space
        txt_f[vars0[i,row]] <- vars0[i, substr_col0(txt_f, paste0("", formatFrtn(nv[1], width=8, max_dec=6)), ln=row, col=(sp=="SP2")+1)] 
      }
      # Export the updated file
      writeLines(txt_f, paste0(folders$apex, apex_fn))
    }
    return(vars0)
  } else {
    if (lvars %like% "\\<parm\\>") {
      apex_fn <- "PARM1501.DAT"
      # Get the information
      parm <- read_param_file(folder = folder)$parm
      nm <- gsub("^.*\\<([0-9]+)\\>.*$", "\\1", lvars)
      sp <- gsub("^.*\\<(SP[12])\\>.*$", "\\1", lvars)
      vars0 <- cbind(fn="PARAM1501.DAT", parm[id==nm, .(row, col=pos, value)])
      # adjust the information?
      if (!is.null(nv)) {
        txt_f <- vars0[,readLines(paste0(folders$apex, apex_fn))]
        for (i in 1:nrow(vars0)) {
          # using 2 decimal places and only 7 out of 8 spaces to keep an empty space
          txt_f[vars0[i,row]] <- vars0[i, substr_col0(txt_f, paste0("", formatFrtn(nv[1], width=8, max_dec=6)), ln=row, col)] 
        }
        # Export the updated file
        writeLines(txt_f, paste0(folders$apex, apex_fn))
      }
      return(vars0)
    } else {
      # Generate the filter
      filter <- if (length(lvars) == 1) lvars else paste0("^(", paste0(lvars,collapse="|"), ")$")
      # Load the structure
      vars0 <- load_apex_file_str()[Parameter %like% filter,.(apex_fn, Line, ColPos, NumRows, Parameter, DataType, ColMin, ColMax, Dec)]
      if (nrow(vars0) == 0) {
        message("No variable matches the names listed in lvars.")
        return(NULL)
      }
      # expand the associated files if needed
      vars1 <- vars0[, by=.(apex_fn,Parameter), 
                     {fn <- if (apex_fn %like% "[xy]\\.") {
                       dir(folder, pattern=gsub("^[xy]", "", apex_fn))
                     } else apex_fn
                     .(fn=fn, Line, ColPos, NumRows, DataType, ColMin, ColMax, Dec)}]
      # Check that all files were found
      if (nrow(vars1[is.na(fn)])) {
        stop("Unable to find files associated with: ", 
             paste0(vars1[is.na(fn), unique(apex_fn)], collapse=", "))
      }
      # Repeated values?
      if (!multi) vars1[,NumRows := NA]
      # read the values
      res0 <- vars1[,by=.(Parameter, apex_fn, fn), {x<-apex_read(fn, Line, ColPos, reps=NumRows, folder); x[,fn:=NULL]}]
      if (nrow(res0[is.na(as.numeric(value0)) != is.na(value0)])) {
        message("Some non-numeric values")
      } else {
        res0[, value0 := as.numeric(value0)]
      }
      # transpose if needed and possible
      if (multi && transpose & nrow(res0) > 0) {
        res0[!is.na(reps), xid := (ln-ln0)/reps + 1]
        res0 <- dcast.data.table(res0, fn + xid ~ Parameter, value.var="value0")[,
                                                                                 c("fn", vars0$Parameter), with=FALSE]
      }
      
      # something to adjust?
      if (length(lvars)==1 & !is.null(nv)) {
        txt_f <- vars0[,readLines(paste0(folders$apex, apex_fn))]
        # if (tolower(vars0$DataType) %in% c("long", "int")) format <- paste0("%", vars0[,ColMax-ColMin+1],"d")
        # if (tolower(vars0$DataType) %in% c("float", "int")) format <- paste0("%", vars0[,ColMax-ColMin+1],".", vars0$dec,"d")
        # if (vars0$DataType %like% "^text") format <- paste0("%", vars0[,ColMax-ColMin+1],"s")
        for (i in 1:nrow(vars0)) {
          txt_f[vars0[i,Line]] <- vars0[i, substr_col0(txt_f, paste0("", formatFrtn(nv[1], width=ColMax-ColMin+1, max_dec=nchar(Dec))), 
                                                       ln=Line, col=ColPos)]
        }
        # Export the updated file
        writeLines(txt_f, paste0(folders$apex, vars0[1, apex_fn]))
      }
      return(res0)
      
    }
  }
}

# Using the apex structure, read the original values of a parameter file
load_in_param_files <- function(fn, folder=NULL) {
  load_in_params(load_apex_file_str()[apex_fn == fn & !is.na(Parameter), Parameter], multi=TRUE, transpose = FALSE, folder=folder)
}


# Load a file with a structure defined in the apex structure table
read_fwf_table <- function(fn, start_line=1, fn_filter=NA, transform_labels=TRUE, folder=NULL) {
  # Setting the folder
  if (is.null(folder)) {
    folder <- if (exists("folders")) folders$apex else "."
  } else {
    folder <- folder[1]
  }
  if (is.na(fn_filter)) fn_filter <- fn
  st <- load_apex_file_str()[apex_fn==fn_filter & Line == start_line,
                             .(Parameter, ColMin, ColMax, width=ColMax-ColMin+1)]
  res <- as.data.table(read.fwf(paste0(folder, fn), widths=st$width, sep="\001", ### sep is needed to make this work. Using an unlikely value here
                                col.names=st$Parameter, skip=start_line-1))
  # Transform categorical values
  if (transform_labels) {
    labs <- read_parameter_values()
    for (i in intersect(names(res), unique(labs$Parameter))) {
      d1 <- setdiff(res[[i]], labs[Parameter==i, Value])
      if (length(d1)) {
        warning("For parameter ", i, ", these values are undefined: ", paste0(d1, collapse= ", "))
      }
      res[[i]] <- factor(res[[i]], labs[Parameter==i, Value], labs[Parameter==i, Description])
    }
  }
  return(res)
}

# read TILLCOM.DAT file
read_tillcom <- function(folder=NULL) {
  if (is.null(folder)) {
    folder <- folders$apex
    if (!file.exists(paste0(folder, "TILLCOM.DAT"))) {
      if (!dir.exists(folder)) {
        dir.create(folder, recursive=T)
        message("APEX directory created.")
      }
      if (!file.copy("../../apex_r/supporting files/apex_pre/TILLCOM.DAT", folder)) {
        stop("Error: Can't find file TILLCOM.DAT")
      } 
    }
  }
  read_fwf_table("TILLCOM.DAT", 3, folder=folder)
}

# read CROPCOM.DAT file
read_cropcom <- function(folder=NULL, fn="CROPCOM.DAT") {
  if (is.null(folder)) {
    folder <- folders$apex
    if (!file.exists(paste0(folder, fn))) {
      if (!dir.exists(folder)) {
        dir.create(folder, recursive=T)
        message("APEX directory created.")
      }
      if (!file.copy("../../apex_r/supporting files/apex_pre/CROPCOM.DAT", 
                     paste0(folder, fn))) {
        stop("Error: Can't find file CROPCOM.DAT")
      } 
    }
  }
  x <- read_fwf_table(fn, 3, folder=folder, fn_filter="CROPCOM.DAT")
  as.data.table(lapply(x, function(x) if (class(x)[1]=="character") trimws(x) else x))
}

read_opc <- function(folder=NULL) {
  # Setting the folder
  if (is.null(folder)) {
    folder <- if (exists("folders")) folders$apex else "."
  } else {
    folder <- folder[1]
  }
  lf_opc <- dir(folder, ".OPC$", ignore.case = TRUE)
  df_opc <- lapply(lf_opc, function(x) read_fwf_table(x, 3, "x.OPC"))
  names(df_opc) <- lf_opc
  rbindlist(df_opc, idcol="fn")
}

# Merging an n-row column names into one column name vector
n_row_column <- function(txt, spaces="[- |]") {
  # Force spaces
  txt <- sapply(txt, gsub, pattern=spaces, replacement=" ", USE.NAMES = FALSE)
  # Dummy variable
  txt0 <- format(txt[1], width = max(nchar(txt)), justify="left")
  # And assign x to fill gaps
  for (j in 2:length(txt)) {
    for (i in 1:nchar(txt[j])) if (substr(txt[j], i, i) != " ") substr(txt0, i, i) <- "x"
  }
  ptx <- c(0, gregexpr("[[:alnum:]#][[:space:]]",txt0)[[1]], nchar(txt0))
  # extract the substrings
  r0 <- sapply(2:(length(ptx)), function(x) trimws(substr(txt, ptx[x-1]+1, ptx[x])))
  # and combine them
  return(apply(r0,2,paste0, collapse=""))
}

# Get the columns and the rows to skip. Search focuses on the first 300 lines
get_row_column <- function(fn, pattern, crn = 1, expand_spaces=TRUE, lines_focus=300) {
  if (crn < 1) stop("crn out of range")
  txt <- readLines(fn, lines_focus)
  # if needed, adapt the pattern
  pattern <- gsub("([][\\().])", "\\\\\\1", pattern)
  if (expand_spaces) pattern <- gsub(" +", " +", pattern)
  cr <- grep(pattern, txt)
  if (length(cr) == 0) {
    return(list(col.names=NULL, skip="__auto__"))
  } 
  if (length(cr) > 1) message("Multiple rows match pattern. Returning the first one.")
  if (crn == 1) {
    txt <- txt[cr[1]]
    cn <- strsplit(txt, "[[:space:]]+")[[1]]
  } else {
    # Combining both row-columns
    cn <- n_row_column(txt[(cr[1]-1):cr[1]])
  }
  # Removing empty cells at the beginning and at the end
  if (cn[1] == "") cn <- cn[-1]
  if (cn[length(cn)] == "") cn <- cn[-length(cn)]
  # Done
  return(list(col.names=cn, skip=cr[1], n=length(cr)))
}

# Read several types of files following the structure defined in APEX_Output_files
read_out_files <- function(extension, folder=NULL, forceImport = FALSE) {
  # if (extension==".DCN") browser()
  # Setting the folder
  if (is.null(folder)) {
    folder <- if (exists("folders")) folders$apex else "."
  } else {
    folder <- folder[1]
  }
  # Read the format
  off <- read_out_files_format()[Extension == extension]
  # Find the files
  lf <- dir(folder, paste0(extension, "$"), ignore.case = TRUE)
  if (length(lf) == 0) {
    message("No files found.")
    return(NULL)
  }
  names(lf) <- basename(lf)
  # read the files according the type
  if (off$Structure == "table") {
    if (off$Delim == "space") {  # If space works well, use fread
      if (any(lf %like% "\\.SAD$")) {
        warning(".SAD cropnames had issues! Be careful before using the data!")
      }
      
      # Adapt ColumnRowID
      lheaders <- lapply(lf, function(x) get_row_column(paste0(folder, x), 
                                                        off$ColumnRowID, off$ColumnRows))
      # Checking options
      res <- mapply(function(fn, headers) {
        if (length(headers$col.names)==0) {
          message("Row-column marker (%s) not found in %s.\n", off$ColumnRowID, x)
          return(NULL)
        } else {
          if (headers$n>1) {
            message("Several row-columnn markers (%s) in %s.\n", off$ColumnRowID, x)
            return(NULL)
          } else {
            xx <- fread(paste0(folder, fn), skip=headers$skip)
            setnames(xx, gsub("[()]", "", headers$col.names[1:ncol(xx)]))
            if (length(headers$col.names) != ncol(xx)) {
              message(sprintf("%s: Mismatch between headers and data.", fn))
            }
            return(xx)
          }
        }
      }, lf, lheaders, SIMPLIFY = FALSE)
      res <- res[sapply(res, function(x) !is.null(x))]
      res <- rbindlist(res, idcol="file")
    } else {
      # Checking options
      res <- rbindlist(lapply(lf, function(x) read_fwf_ad(paste0(folder, x), 
                                                          skip=gsub(" +", " +", off$ColumnRowID),
                                                          fixed=FALSE)), 
                       idcol="file")
      
    }
    
  } else {
    fun_nm <- paste0("read_apex_", gsub("\\.", "", extension))
    fun <- getFunction(fun_nm, mustFind = FALSE)
    if (is.null(fun)) {
      message(extension, " format not yet supported.")
      return(NULL)
    }
    res <- fun(lf, off$ColumnRowID, off$ColumnRows, folder=folder)
    if (is.null(res)) return(NULL)
  }
  cn <- check_column_names(names(res), off$Extension)
  if (nrow(cn[final %like% "\\|" | !exists])) {
    message("Problem with column names!")
    if (!forceImport) {
      return(cn)
    } else {
      message("Check the columns for which exists==FALSE")
      print(cn)
    }
  } 
  # Adjusting names
  setnames(res, cn[exists==TRUE, initial], cn[exists==TRUE, final])
  # Adjusting other issues/problems
  if (all(c("Day", "Month", "Year") %in% names(res))) {
    res[, date := as.IDate(paste(Year, Month, Day, sep="-"))]
  } else {
    if (all(c("Month", "Year") %in% names(res))) {
      # browser()
      res[, date := as.IDate(paste(Year, Month, "15", sep="-"))]  # Using the 15th to represent every month
    } else {
      if ("Year" %in% names(res)) {
        res[, date := as.IDate(paste(Year, "-07-01", sep=""))]  # Mid-year
      }
    }
  }
  # Reviewing RFV and PRCP problem. Both should be the same, but sometimes
  # one is empty
  if ("RFV" %in% names(res)) {
    if (!("PRCP" %in% names(res))) {
      setnames(res, "RFV", "PRCP")
    } else {
      if (sum(res$RFV)==0) res[,RFV := NULL]
      if (sum(res$PRCP)==0) res[,PRCP := NULL]
      if ("RFV" %in% names(res) & "PRCP" %in% names(res)) {
        message("RFV and PRCP with non-zero information, keeping both.")
      } else {
        if ("RFV" %in% names(res)) setnames(res, "RFV", "PRCP")
      }
    }
  }
  # Fixing extra-spaces in names
  if ("CPNM" %in% names(res)) res[, CPNM := trimws(CPNM)]
  
  # Checking duplicated initial conditions (e.g., in DCN the first record is duplicated, I'm assuming it represents initial simulation conditions)
  id_vars <- intersect(names(res), c("file", "date", "Year", "Month", "Day", "SAID", "CPNM", "Layer"))
  # browser()
  # SAO can have duplicated NA values because CPNM. 
  if (extension %in% c(".SAO", ".MGZ")) {
    x <- res[!is.na(CPNM) & CPNM != ""][,keyby=id_vars, .(n=.N)][n>1]
  } else {
    x <- res[,keyby=id_vars, .(n=.N)][n>1]
  }
  if (nrow(x)) {
    # Report if it is an unknown type
    if (extension %in% c(".DCN") + (nrow(x[n>2])==0)) {
      # Double check problem beyond first row
      x1 <- res[,by=file,.(date=min(date))]
      if (nrow(x[x1, on="file"][date!=i.date])) {
        message("Unexpected structure! Review ", extension, "files!")
      } else {
        # Finding the values to be offset
        ldup1 <- x[res,on=id_vars,n]              # dates to be modified
        ldup0 <- !duplicated(res[,id_vars,with=F]) # dates to be substracted
        ldup0[is.na(ldup1)] <- FALSE
        res[ldup0==TRUE, date := date - 1]
        res[ldup0==TRUE, c("Year", "Month", "Day") := .(year(date), month(date), mday(date))]
      }
    } else {
      message("Potential duplicated records in ", extension)
      print(x)
      # browser(NULL)
    }
  }
  
  return(res)
}

# Function to deal with complex records 
capture_record <- function(x, lmt_txt) {
  # Remove empty lines
  x1 <- x[!grepl("^ *$", x)]
  # split the record in two by lmt_txt
  lmt <- grep(lmt_txt, x1)
  if (length(lmt) == 0) {
    message("Only working on records delimited by '\\n' and with a header delimited by ", lmt_txt)
    return(NULL)
  }
  # Working on the header
  hd <- paste0(x1[1:(lmt-1)], collapse=" ")
  dt <- regmatches(hd, regexec("([0-9]{4}) +([0-9]{1,2}) +([0-9]{1,2})", hd))
  sa_n <- regmatches(hd, regexec("SA#= +([0-9]+)", hd))
  sa_id <- regmatches(hd, regexec("ID= +([0-9]+)", hd))
  # Checking the data: SA always present, date sometimes only
  if (length(sa_n) != 1 | length(sa_id) != 1 | length(dt) > 1) {
    message("Inconsistent header:")
    message(paste0(x1[1:(lmt-1)], collapse="\n"))
    return(NULL)
  }
  res <- data.table(SAID=as.integer(sa_id[[1]][2]), NBSA=as.integer(sa_n[[1]][2]))
  if (length(dt[[1]]) >= 1) {
    res[, c("YR", "MO", "DAY") := as.list(as.integer(dt[[1]][-1]))]
  }
  # Working on the table section
  # -- Problem with "BD 33KPA"... has a space. also, we need to add a "layer" variable, to 
  #    capture the first row of values. (can't be column names, as numbers can change from
  #    soil to soil)
  x2 <- paste0("variable", paste0(x1[-1:-(lmt)], collapse="\n"))
  x2 <- gsub("BD 33KPA", "BD_33KPA", x2)
  x2 <- gsub("CO2 LOSS", "CO2_LOSS", x2)
  x2 <- gsub("NET MN", "NET_MN", x2)
  tb <- fread(x2, fill=TRUE, header=T)
  setnames(tb, raster::validNames(names(tb)))
  # Explore extra labels
  if (nrow(tb[variable %flike% "|"])) {
    nv <- tb[,tstrsplit(variable, "[|=]")]
    uv <- unique(na.omit(nv$V2))
    if (ncol(nv) > 3 | length(uv) > 1) stop("Code not prepared to deal with more than one category")
    lv <- c("variable", uv)
    setnames(nv, c("V1", "V3"), lv)
    tb <- cbind(nv[,-"V2"], tb[,-"variable"])
  } else {
    lv <- "variable"
  }
  # Reorganizing the table
  tb1 <- melt(tb, id.vars=lv, variable.name = "Layer")
  tb2 <- dcast(tb1, ... ~ variable, value.var = "value")
  return(cbind(res, tb2))
}



# Functions to deal with special formats in some files
# .MWS files
read_apex_MWS <- function(lf, ColumnRowID, ColumnRows, folder=NULL) {
  # browser()
  # Setting the folder
  if (is.null(folder)) {
    folder <- if (exists("folders")) folders$apex else "."
  } else {
    folder <- folder[1]
  }
  # Get the headers
  lheaders <- lapply(lf, function(x) get_row_column(paste0(folder, x), 
                                                    ColumnRowID, ColumnRows))
  # reading based on predefined widths
  res <- rbindlist(mapply(function(fn, hd) fread(paste0(folder, fn),
                                                 skip=hd$skip, header = FALSE, fill=TRUE), 
                          lf, lheaders, SIMPLIFY = FALSE), 
                   idcol="file")
  # Compare headers and data read.
  if (length(lheaders[[1]]$col.names) != (ncol(res)-3)) {
    message("MWS file column number does not match file data.")
    return(NULL)
  }
  setnames(res, c("file","variable",lheaders[[1]]$col.names, "varX"))
  # Reorganize the data
  res[,varX := NULL]
  res[,isyear:=grepl("[0-9]{4}", variable)]
  res[,yid := cumsum(isyear)]
  res[,by=yid,YEAR := as.integer(grep("^[0-9]{4}$", variable, value = TRUE))]
  res0 <- res[isyear==FALSE]
  res0[,c("yid", "isyear") := NULL]
  # Using numbers for month names
  setnames(res0, c("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG",
                  "SEP", "OCT", "NOV", "DEC"), sprintf("%02d", 1:12))
  # Removing accumulated annual value
  if ("YR" %in% names(res0) & class(res0$YR)=="numeric" & "YEAR" %in% names(res0)) res0[,YR := NULL]
  res1 <- melt.data.table(res0, measure.vars=sprintf("%02d", 1:12),
                          variable.factor = FALSE, variable.name = "MONTH")
  res1[,MONTH := as.integer(MONTH)]
  res <- dcast(res1, file + YEAR + MONTH ~ variable)#, id.vars="value")
  # ready!
  return(res)
}

# .ACO files
read_apex_ACO <- function(lf, ColumnRowID, ColumnRows, folder=NULL) {
  # Setting the folder
  if (is.null(folder)) {
    folder <- if (exists("folders")) folders$apex else "."
  } else {
    folder <- folder[1]
  }
  # Get the headers
  lheaders <- lapply(lf, function(x) get_row_column(paste0(folder, x), 
                                                    ColumnRowID, ColumnRows))
  # Checking format issues
  if (length(unique(sapply(lheaders, function(x) length(x$headers)))) > 1) {
    message("Inconsistent ACO files' format.")
    return(NULL)
  }
  # reading based on predefined widths
  res <- rbindlist(mapply(function(fn, hd) read.fwf(paste0(folder, fn),
                                                    widths=c(9, 8, 5, 2, 2, 18,
                                                             5,4,4,4,4,
                                                             10,10,10,10,10),
                                                    skip=hd$skip, header = FALSE), 
                          lf, lheaders, SIMPLIFY = FALSE), 
                   idcol="file")
  # Compare headers and data read.
  if (length(lheaders[[1]]$col.names) != (ncol(res)-1)) {
    message("ACO file column number does not match file data.")
    return(NULL)
  }
  setnames(res, c("file",gsub("[()]","",lheaders[[1]]$col.names)))
  return(res)
}

# .DUX files
read_apex_DUX <- function(lf, ColumnRowID, ColumnRows, folder=NULL) {
  # Setting the folder
  if (is.null(folder)) {
    folder <- if (exists("folders")) folders$apex else "."
  } else {
    folder <- folder[1]
  }
  # Get the headers
  lheaders <- lapply(lf, function(x) get_row_column(paste0(folder, x), 
                                                    ColumnRowID, ColumnRows))
  # Checking format issues
  if (length(unique(sapply(lheaders, function(x) length(x$headers)))) > 1) {
    message("Inconsistent DUX files' format.")
    return(NULL)
  }
  # reading based on predefined widths
  res <- rbindlist(mapply(function(fn, hd) read.fwf(paste0(folder, fn),
                                                    widths=c(9, 8, 5, 2, 2, 
                                                             4,4,4,13,
                                                             6,8,8,8,8,8),
                                                    skip=hd$skip, header = FALSE), 
                          lf, lheaders, SIMPLIFY = FALSE), 
                   idcol="file")
  # Compare headers and data read.
  if (length(lheaders[[1]]$col.names) != (ncol(res)-1)) {
    message("DUX file column number does not match file data.")
    return(NULL)
  }
  setnames(res, c("file",gsub("[()]","",lheaders[[1]]$col.names)))
  return(res)
}

# .SCX files
read_apex_SCX <- function(lf, folder=NULL, ...) {
  read_apex_ACN(lf, folder=NULL, ...)
}

# .DCN files
read_apex_DCN <- function(lf, folder=NULL, ...) {
  read_apex_ACN(lf, folder=NULL, ...)
}

# .ACN files
read_apex_ACN <- function(lf, folder=NULL, ...) {
  # Setting the folder
  if (is.null(folder)) {
    folder <- if (exists("folders")) folders$apex else "."
  } else {
    folder <- folder[1]
  }
  d1 <- rbindlist(lapply(lf, function(fn) {
    txt0 <- readLines(paste0(folder, fn))
    empty <- grepl("^ *$", txt0)
    
    txt1 <- split(txt0, cumsum(empty))
    txt1 <- txt1[sapply(txt1, function(x) !(length(x)==1 & x[1]==""))]
    # Capture records (but dropping the first one)
    txt2 <- rbindlist(lapply(txt1[-1], capture_record, "SOIL LAYER"), fill=TRUE)
  }), idcol="file")
}

# .RCH files
read_apex_RCH <- function(lf, ColumnRowID, ColumnRows, folder=NULL, ...) {
  return(read_apex_SAO(lf, ColumnRowID, ColumnRows, folder, ...))
}

# .MGZ files
read_apex_MGZ <- function(lf, ColumnRowID, ColumnRows, folder=NULL, ...) {
  return(read_apex_SAO(lf, ColumnRowID, ColumnRows, folder, ...))[!is.na(CPNM)]  # Provisional solution to NA values.
}

# .SAO files (also works for RCH)
read_apex_SAO <- function(lf, ColumnRowID, ColumnRows, folder=NULL, ...) {
  # Setting the folder
  if (is.null(folder)) {
    folder <- if (exists("folders")) folders$apex else "."
  } else {
    folder <- folder[1]
  }
  # Get the headers. But we need to read the headers from the main table. If not, it won't recognize different columns in different rows
  lheaders <- lapply(lf, function(x) 
    get_row_column(paste0(folder, x), ColumnRowID, ColumnRows)
  )
  
  # # Checking format issues
  # if (length(unique(sapply(lheaders, function(x) length(x$headers)))) > 1) {
  #   message("Inconsistent SAO files' format.")
  #   return(NULL)
  # }
  
  # crop.col.names <- c("YLDG", "YLDF", "HUI", "LAI", "RD", "RW", "BIOM", "STL", 
#                     "CPHT", "STD", "STDL", "WS", "NS", "PS", "KS", "TS", "AS", 
  #                     "SALT", "REG", "CPNM")
  # setnames(sao, c("type", "SAID", "Y", "JD",
  #                 names(sao)[5:(which(names(sao)==crop_col_names[1])[1]-1)],
  #                 as.character(outer(crop_col_names, 1:sum(names(sao)==crop_col_names[1]), paste, sep="_"))))
  
  
  # Reading files
  fread_quiet <- purrr::quietly(fread)
  readLines_quiet <- purrr::quietly(readLines)
  res <- mapply(function(fn, hd) {
                          if (hd$n != 1) stop("Error, unexpected column name format in ", fn)
                          # Reading the text file including the header
                          x0 <- readLines_quiet(paste0(folder, fn))
                          x <- x0$result[-(1:(hd$skip-1))]
                          # adjusting the first row to force the column names match the structure
                          if (length(x) < 2) {
                            message("Empty file: ", fn)
                            x2 <- NULL
                          } else {
                            if ((grepl("^ +SAID", x[1]) & grepl("^BIGSUB ", x[2])) |
                                (grepl("^ +RCID", x[1]) & grepl("^REACH ",  x[2]))) {
                              x[1] <- paste("TYPE", x[1])
                            } else {
                              if (!(fn %flike% ".MGZ")) stop("Error, unexpected format in ", fn)
                            }
                            
                            # accommodating the data
                            x2 <- fread_quiet(text=x, fill=TRUE)
                            # Rows that begin with AVEAN are multi annual averages. 
                            # But they follow a different format.
                            # AVEAN can appear in any of the first two columns
                            if ("TYPE" %in% names(x2$result)) {
                              x2$result <- x2$result[TYPE != "AVEAN"] 
                              x2$result[, TYPE := NULL]
                            } else {
                              x2$result <- x2$result[!(x2$result[[1]] == "AVEAN" | 
                                                       x2$result[[2]] == "AVEAN")]
                            }
                            # compiling warning messages
                            if (x0$output == "") x0$output <- character()
                            if (x2$output == "") x0$output <- character()
                            x2$warnings <- list(readLines=x0[c("output", "warnings", "messages")], 
                                                fread=x2[c("output", "warnings", "messages")])
                            x2$output <- NULL
                            x2$messages <- NULL
                          }
                          
                          return(x2)
    }, lf, lheaders, SIMPLIFY = FALSE)
  # Any unpredictable warning?
  warns <- rbindlist(lapply(res, function(x) data.table(warns=unlist(x$warnings))), idcol="file")
  warns <- warns[warns != ""]
  if (nrow(warns[!(warns %flike% "First discarded non-empty line: <<AVEAN" | 
                   warns %flike% "Discarded single-line footer: <<AVEAN")])) {
    message("Unexpected warnings:")
    print(warns)
    if (nrow(warns) > 100) {
      fwrite(warns, sep="\t", file=paste0(folder, "/SAO_warns.txt"), row.names = FALSE, na="")
      message("Too many warnings to be displayed. Warnings copied to ", 
              folder, "/", gsub("^.*\\.(.*)$", "\\1", lf),"_warns.txt")
    }
    if (nrow(warns[!(warns %like% "appears to contain an embedded nul")])) {
      return(NULL)
    }
  }
  res <- res[!sapply(res, is.null)]
  # Up to 5 crops reported per subwatershed
  res1 <- lapply(res, function(x) {
    w <- x$result
    # Column names are disorganized. Fixing them
    col.names <- remove_units(names(w))
    # Adding type column and replacing GIS column name with YEAR
    col.names[col.names=="GIS"] <- "YR"
    col.names[col.names=="TIME"] <- "JDA"
    # Checking multiple crops, and fixing headers
    col.names <- unique_names(col.names)
    setnames(w, col.names)
    # Any duplicated column to take care of?
    lc <- cbind(col.names, as.data.table(tstrsplit(col.names, "\\.")))
    if (ncol(lc)==3) {
      lc[,V2 := as.integer(V2)]
      w0 <- lapply(1:max(lc$V2, na.rm = TRUE), function(i) {
        # Extract the relevant columns only
        x0 <- w[,with=FALSE, lc[is.na(V2) | V2 == i, col.names]]
        # adjust the col names
        setnames(x0, gsub(paste0("\\.", i), "", names(x0)))
        # return
        return(x0)
      })
      w <- rbindlist(w0, use.names = TRUE, fill=TRUE, idcol="crop_col_id")
    }
    return(w)
  })
  
  res <- rbindlist(res1, use.names = TRUE, idcol="file")

  
  # Updating dates
  if ("JDA" %in% names(res)) {
    # Checking them first
    if (nrow(res[,by=YR, .(fd=min(JDA)==1, ld=(max(JDA)-((YR %% 4)==0)==365))][!(fd & ld)])) stop("Review date format!")
    # All good with leap years. Transforming the data
    res[,date:=as.IDate(sprintf("%d-01-01", YR))+(JDA-1)]
    res[, c("MONTH", "DAY") := .(month(date), mday(date))]
    res[, date := NULL]
  } else {
    if ("MO" %in% names(res)) {
      setnames(res, "MO", "MONTH")
    }
  }
  # Transforming data if needed
  x1 <- sapply(res, class)
  # Non numeric?
  sc <- setdiff(names(x1[x1 == "character"]), c("file", "CPNM"))
  if (length(sc)) {
    message("Unexpected character variables.")
    invisible(lapply(sc, function(x) {
      y <- unique(res[[x]])
      y <- y[!grepl("^ *-?[0-9]+\\.?[0-9]* *$", y)]
      if (length(y)) message("Confirm transforming non-numeric values present in ", x, " to NA is fine: '",  
                             paste(sort(y), collapse="', '"), "'.\n")
      set(res, j = x, value=as.numeric(res[[x]]))
    }))
  }
  setcolorder(res, intersect(c("file", "crop_col_id", "SAID", "YR", "MONTH", "DAY"), names(res)))
  return(res)
}

# Transform a text like the following
"   PER =-0.296838D-13  DF  =-0.145519D-10  BRSY= 0.000000D+00  YS  = 0.491850D+05  YW  = 0.490230D+05  DEP = 0.163754D+03
    DEG = 0.175713D+01  RSYF= 0.000000D+00"
# In a named vector
interpret_vector <- function(txt) {
  # removing "\n" and collapsing the text
  txt0 <- paste0(gsub("\n", " ", txt), collapse=" ")
  # Removing spaces around '='
  txt0 <- gsub(" *= *", "=", txt0)
  # so, empty spaces should be replaced by commas and 'D' with 'E'
  txt0 <- gsub(" +", ", ", trimws(gsub("([0-9+.-])D([-+0-9])", "\\1E\\2", txt0)))
  # parse it 
  txt1 <- try(eval(parse(text = paste0("c(", txt0, ")"))), silent=TRUE)
  if (class(txt1)=="try-error") {
    warning(paste0("Error parsing:\n", paste0(txt, collapse="\n")))
    return(NULL)
  }
  return(txt1)
}


# Extract a section of a file or a character vector
# start_str and end_str are the markers
# start_mode and end_mode change the line counts +/-
extract_between <- function(fn, start_str, end_str, start_mod=1, end_mod=-1, add_end = FALSE) {
  # Read the file
  txt <- if (length(fn) > 1) fn else readLines(fn)
  # Finding the strings
  start <- grep(start_str, txt)
  if (start_str == end_str) {
    end <- start[-1]
    if (add_end) {
      end <- c(end, length(txt))
    } else {
      start <- start[-length(start)]
    }
  } else {
    end <- grep(end_str, txt)
    if (add_end) end <- c(end, length(txt))
  }
  if (length(start) != length(end)) {
    message("Number of times start and end string match should be the same.")
    return(NULL)
  }
  if (length(start) < 1) {
    message("No matches.")
    return(NULL)
  }
  start <- start + start_mod
  end[1:(length(end)-add_end)] <- end[1:(length(end)-add_end)] + end_mod
  if (any(start[-1] <= end[-length(end)]) | any(start > end)) {
    warning("Overlapping or misalign sections.")
  }
  mapply(function(x, y) txt[(x+start_mod):(y+end_mod)], start, end, SIMPLIFY = FALSE)
}


# Get a table, export in the adequate format
# - Get the table in the right order with one row per data to be included as an APEX file
# - Open database_structure, subset for the specific output
# - Keep the columns that match the database_structure
# - Add other columns with default values
# - Any missing values? Values out of range?
# - Recover the value position (row + col + width)
# - Export the table
write_apex_inputs <- function(tb, fmt_name, by=NULL, fn=NULL, append=FALSE, lines=NULL) {
  tb0 <- copy(tb)
  setDT(tb0)
  if (is.null(fn)) fn <- fmt_name
  if (is.null(by)) if (haskey(tb0)) by <- key(tb0) else stop("No key in tb, neither in by")
  # - Open database_structure, subset for the specific output
  fmt <- load_apex_file_str()[apex_fn==fmt_name & !is.na(Parameter)]
  # Focusing on relevant lines only
  if (!is.null(lines)) fmt <- fmt[Line %in% lines]
  # Check format
  fmt[,DataType0 := gsub("\\([0-9]+\\)", "", tolower(DataType))]
  fmt[,DataType0 := c(long="d", int="d", float="f", exp="E", text="s")[DataType0]]
  fmt[,digits0 := nchar(Dec)]
  fmt[,width0 := ColMax-ColMin+1]
  fmt[is.na(digits0), digits0 := width0]
  fmt[Default == "<< empty >>" & DataType0=="s", Default := ""]
  # if (nrow(fmt[ColMax-ColMin < nchar(Dec)])) stop("Problem with format in ", 
  #                                                 fmt[ColMax-ColMin < nchar(Dec), 
  #                                                     paste0(Parameter, collapse = ", ")])
  # - Keep the columns that match the database_structure
  tb1 <- tb0[, with=FALSE, union(by, intersect(names(tb0), fmt$Parameter))]
  # - Add other columns with default values
  for (i in setdiff(fmt$Parameter, names(tb1))) {
    Def0 <- fmt[Parameter == i, Default]
    fun <- switch(fmt[Parameter == i, DataType0], d=as.integer, f=as.numeric, 
                  E=as.numeric, s=as.character)
    tb1[[i]] <- if (length(fun)==0) fmt[Parameter == i, Def0] else fmt[Parameter == i, fun(Def0)]
  }
  # - Check all values are in range
  oor <- fmt[, by=Parameter,{
    .(out_of_range=any(((tb1[[Parameter]] < Min) | (tb1[[Parameter]] > Max)) & tb1[[Parameter]] != Default),
      missing = Required %in% c("Key", "Yes") & any(is.na(tb1[[Parameter]])))
  }]
  if (nrow(oor[out_of_range == TRUE])) {
    message("Values out of range in columns:", paste0(oor[out_of_range == TRUE, Parameter], collapse=", "))
  }
  if (nrow(oor[missing == TRUE])) {
    message("Missing values in columns:", paste0(oor[missing == TRUE, Parameter], collapse=", "))
  }
  rm(oor)
  #tb1x <- copy(tb1)
  # Adjusting the format
  x <- fmt[, by=Parameter,{
    if (DataType0 == "s") {
      y <- sprintf(paste0(" %-",width0-1,"s"), tb1[[Parameter]])#, format="s", digits=digits0-1, width=width0-1, justify="left")
      #cat(Parameter, " ", unique(nchar(y)), " ", unique(width0), " ", unique(digits0), "\n")
      tb1[[Parameter]] <<- y
    } else {
      tb1[[Parameter]] <<- formatC(tb1[[Parameter]], format=DataType0, digits=digits0, width=width0)
    } 
  }]
  #browser()
  # - Expand the table
  tb2 <- melt.data.table(tb1[, with=FALSE, c(by, fmt$Parameter)], measure.vars=fmt$Parameter, 
                         id.vars=by, 
                         variable.factor = FALSE)[fmt[, .(Parameter, Line, ColMin, ColMax, ColPos)], 
                                                  on=c(variable="Parameter"), nomatch=0]
  
  if (nrow(tb2[,by=c(by, "Line", "ColPos"), .(n=.N)][n>1]) > 0) {
    message("\nError: Multiple values for the following combinations of key values, line, and position:")
    print(tb2[,by=c(by, "Line", "ColPos"), .(n=.N)][n>1])
    stop()
  }
  # Exporting the text file
  if (!dir.exists(folders$apex)) dir.create(folders$apex, recursive=TRUE)
  # - Recover the value position (row + col + width)
  setkeyv(tb2, c(by, "Line", "ColPos"))
  # Open the connection (to append if needed)
  if (fn == basename(fn)) fn <- paste0(folders$apex, fn)
  con1 <- file(fn, open=if(append[1]) "a" else "")
  # Write the lines
  writeLines(tb2[,by=c(by, "Line"), .(txt=paste0(value, collapse=""))]$txt, con=con1)
  close(con1)
}

read_fertcom <- function(fdata = NULL, folder=NULL) {
  
  # if (!file.exists(paste0(folders$apex, "FERTCOM.DAT"))) {
  #   if (!dir.exists(folders$apex)) {
  #     dir.create(folders$apex, recursive=T)
  #     message("APEX directory created.")
  #   }
  #   if (!file.copy("../../apex_r/supporting files/apex_pre/FERTCOM.DAT", folders$apex)) {
  #     stop("Error: Can't find file FERTCOM.DAT")
  #   } 
  # }
  
  # Setting the folder
  if (is.null(folder)) {
    folder <- if (exists("folders")) folders$apex else "."
  } else {
    folder <- folder[1]
  }
  # Define file name of fertilizer data in apex_in.
  fname <- paste0(folder, "FERTCOM.DAT")
  # Is fdata provided?
  if (is.null(fdata)) {
    # If not, does file exist in apex_in?
    if (!file.exists(fname)) {
      # If file not in apex_in, copy from apex_pre first.
      wk <- file.copy("../../apex_r/supporting files/apex_pre/FERTCOM.DAT",
                      fname, overwrite = T)
      if (!wk) stop("FERTCOM file not copied from apex_pre.")
    } 
    # If file is in apex_in, read from there.
    fdata <- fread(fname)
    setnames(fdata, c("#", "Name"), c("FTID", "FTNM"))
  }
  # If fdata is still not loaded, produce error.
  if (is.null(fdata)) stop("FERTCOM file not properly loaded.")
  return(fdata)
}

add_fert <- function(n, p, k, name=NULL, fdata = NULL){
  if (is.null(fdata)) fdata <- read_fertcom(fdata)
  fert <- paste(n, p, k, sep = "-")
  if (is.null(name)) name <- fert
  # Adding a new fertilizer.
  if (!nrow(fdata[FTNM == name])) {
    newr <- data.table(FTID = fdata[, max(FTID)] + 1,
                       FTNM = name,
                       FN = round(n/100, 3), FP = round(p/100*0.4366, 3), 
                       FK = round(k/100*0.8301, 3),
                       FNO = 0.000, FPO = 0.000, FNH3 = 0.000,
                       FOC = 0.000, FSLT = 0.000, FCST = 0.100)
    # Save the new file.
    write_apex_inputs(newr, "FERTCOM.DAT", by="FTNM", append=TRUE)
    # Update the table
    fdata <- rbind(fdata, newr)
  } else message("This fertilizer already exists!")
  return(fdata)
}

# Get the fertilizer ID corresponding with the N,P,K,etc. fractions
get_fert <- function(n, p, k, 
                     no=NA_real_, po=NA_real_, nh3=NA_real_, 
                     oc=NA_real_, slt=NA_real_, cst=NA_real_, add=FALSE) {
  # Get the data
  fdata <- read_fertcom()
  # Prepare the input
  fertx <- data.table(FN = round(n/100, 3), 
                      FP = round(p/100*0.4366, 3), 
                      FK = round(k/100*0.8301, 3),
                      FNO = round(no/100, 3), 
                      FPO = round(po/100, 3), 
                      FNH3 = round(nh3/100, 3),
                      FOC = round(oc/100, 3), 
                      FSLT = round(slt/100, 3), 
                      FCST = round(cst, 3))
  # Transform NA values into zeros
  invisible(lapply(names(fertx), function(x) set(fertx, which(is.na(fertx[[x]])), j=x, 0)))
  # Transform and everything to strings in fertx and fdata
  invisible(lapply(names(fertx), function(x) set(fertx, j=x, value=sprintf("%5.3f", fertx[[x]]))))
  invisible(lapply(names(fertx), function(x) set(fdata, j=x, value=sprintf("%5.3f", fdata[[x]]))))
  
  # Find unique values
  ferty <- unique(fertx)
  # Control potential problems with cost, assigning the average fertilizer cost
  ferty[as.numeric(FCST)==0, 
        FCST := sprintf("%5.3f", mean(fdata[as.numeric(FCST) !=0, as.numeric(FCST)]))]
  # Check for pre-existent fertilizers (cost is not used in the link, only the others)
  ferty[fdata, on=c("FN", "FP", "FK", "FNO", "FPO", "FNH3", "FOC", "FSLT"), 
        c("FTID", "FTNM") := .(FTID, FTNM)]
  # add missing ones
  to_add <- ferty[,which(is.na(FTID))]
  # Save the new fertilizers
  if (length(to_add) > 0) {
    ferty[to_add, FTID := max(fdata$FTID)+1:.N]
    ferty[to_add, FTNM := paste0("nw",FTID)]
    # Checking no duplicates in name
    pdup <- fdata[FTNM %like% paste0(ferty[to_add, gsub("\\[A-Z]+$", "", FTNM)], collapse="|")]
    # Solve this problem later
    if (nrow(pdup)) {
      stop("Duplicated fertilizer names!")
    }
    message(sprintf("Adding %d fertilizers.", length(to_add)))
    write_apex_inputs(ferty[to_add], "FERTCOM.DAT", by="FTNM", append=TRUE)
    # Read the data again
    fdata <- read_fertcom()
    # And transform to chr as before
    invisible(lapply(names(fertx), function(x) set(fdata, j=x, value=sprintf("%5.3f", fdata[[x]]))))
  }
  
  # and return the fertilizer IDs based on the original list
  return(fdata[fertx, on=c("FN", "FP", "FK", "FNO", "FPO", "FNH3", "FOC", "FSLT"), FTID, nomatch=0])
}

# Adjusting operations:
# - Kill everything before
# - removing duplicated kills
# - removing duplicated plant operations
adjust_ops <- function(tb) {
  tb2 <- copy(tb)
  # Create the counters for each crop in the table
  cr_crop <- cbind(unique(tb2[,.(JX6)])[order(JX6)], alive=0)
  setkey(cr_crop, JX6)
  tb2[, to_delete := FALSE]
  tb2[, op_plant := grepl("^Plant", DESCR, ignore.case = TRUE)]
  tb2[, op_kill  := grepl("^Kill", DESCR, ignore.case = TRUE)]
  #c_plant <- 0L
  # Move row by row
  i <- 1L
  while (i <= nrow(tb2)) {
    # print("\n")
    # print(cr_crop)
    # Is a planting operation? Delete if plant is still alive. If not, make it alive
    if (tb2[i,op_plant]) {
      if (cr_crop[J(tb2[i,JX6]), alive]) {
        tb2[i, to_delete := TRUE]
      } else {
        cr_crop[J(tb2[i,JX6]), alive := 1]
      }
    }
    # Killing operation?
    if (tb2[i,op_kill]) {
      if (!is.na(tb2[i, previous_crop])) {  # Undefined "previous crop"?
        # Any "previous crop" to kill?
        if (nrow(cr_crop[alive > 0])) {
          # Create the list of kills
          r <- tb2[i]
          r[, c("NAME", "JX6") := NULL]
          # For each JX6 of crops alive, get the last NAME and bind with r.
          r1 <- cbind(tb2[i:1][cr_crop[alive > 0], on="JX6"][, by=JX6, .(NAME = NAME[1])], r)
          # Re-build the table
          tb2 <- rbind(tb2[1:(i-1)], r1, 
                       if (i < nrow(tb2)) tb2[(i+1):.N] else NULL, 
                       use.names=TRUE)
          cr_crop[alive > 0, alive := 0]
          # Adjust i
          i <- (i-1) + nrow(r1)
        } else {
          # If nothing alive, remove the row
          tb2[i, to_delete := TRUE]
        }
      } else {
        # Mark crop as dead if alive
        # if (nrow(cr_crop[J(tb2[i,NAME])])>1) browser()
        if (cr_crop[J(tb2[i, JX6]), alive > 0]) {
          cr_crop[J(tb2[i, JX6]), alive := 0]
        } else {
          # If not alive, remove the row
          tb2[i, to_delete := TRUE]
        }
      }
    }
    # Next line
    i <- i + 1L
  }
  # Is the last crop still alive?
  if (cr_crop[.N, alive]) {
    # Find the last operation for that crop.
    last_op <- tb2[JX6%in%cr_crop[.N, JX6]][.N]
    # Is it a harvest? Turn into a kill.
    if (grepl("^Harvest", last_op$DESCR, ignore.case = TRUE)) {
      last_op[,c("DESCR", "JX4", "OPV7") := .("Kill crop", 451, 0)]
      nd <- last_op[,as.IDate(paste0(year_op, "-", JX2, "-", JX3))] + 5 # 5 days later?
      last_op[, c("year_op", "JX2", "JX3") := .(year(nd), month(nd), mday(nd))]
      tb2 <- rbind(tb2, last_op)
      cr_crop[.N, alive := 0]
    } else {
      message("Adjustment may still be needed. Last operation of crop still alive is not harvest.")
    }
      
  }
  # Clean variables
  tb2[, c("op_plant", "op_kill") := NULL]
  return(tb2)
}

date_modify <- function(fd) {
  # Identify the rows with negative values (or NAs, or whatever is the marker)
  tf <- which(fd$JX3 < 0)
  fd2 <- copy(fd)
  if (length(tf)) {
    # from the latest to the first one
    for (x in tf[length(tf):1]) {
      if (x+1 > nrow(fd2)) {  # Are we in the last row?
        fd2[x, c("year_op", "JX2", "JX3") := .(year_op, 12, 31)]
      } else {
        nd <- fd2[x, JX3] + fd2[x+1, as.IDate(paste0(year_op, "-", JX2, "-", JX3))]
        fd2[x, c("year_op", "JX2", "JX3") := .(year(nd), month(nd), mday(nd))]
      }
    }  
  }
  return(fd2[,.(year_op, JX2, JX3)])
}


#### 
force_soil_depth <- function(folder=NULL, margin=0, crop_filter=".*") {
  if (is.null(folder)) folder <- folders$apex
  mxn <- max(read_cropcom(folder)[CROPNAME %like% crop_filter, RDMX]) + margin
  mxt <- sprintf(" %7.2f", mxn)
  if (nchar(mxt) != 8) stop("Error replacing crop depth.")
  lf <- dir(folder, "\\.SOL$")
  # Adjust values if needed
  sapply(lf, function(x) {
    # Read file and clean empty lines
    tx <- readLines(paste0(folder,x))
    tx <- tx[tx!=""]
    # fine the target line
    trg <- (((length(tx)-4)%/%48))*48L + 4L
    if (as.numeric(substr(tx[trg], nchar(tx[trg]) - 7, nchar(tx[trg]))) < mxn) {
      substr(tx[trg], nchar(tx[trg]) - 7, nchar(tx[trg])) <- mxt
      writeLines(tx, paste0(folder,x))
      TRUE
    } else {FALSE}
  })
}

# Run apex from the default folder
# Use error_check=TRUE only if APEX prints "DONE!" in the screen at the end of a successful run.
single_run <- function(folder=NULL, out_to_read=c(".SAO", ".ACY", "out", "out_txt"), apex_app=NULL, 
                       error_check=FALSE) {
  #browser()
  if (is.null(folder)) folder <- folders$apex
  if (is.null(apex_app)) {
    folder <- folders$apex
  } else {
    apex_app <- files$apex_app
    if  (is.null(apex_app)) stop("You need to define apex_app parameter!")
  }
  out <- system(sprintf("cd %s;%s",folder, apex_app),  intern=TRUE)

  # These part deals with blanks added when compiling with gfortran
  lf <- dir(folder, pattern="^ +")
  # delete old files
  unlink(paste0(folder, trimws(lf)))
  file.rename(paste0(folder, basename(lf)), paste0(folder, trimws(lf)))
  
  # Read and format results
  rprt <- list()
  if ("out" %in% out_to_read) {
    out_to_read <- setdiff(out_to_read, "out")
    rprt["out"] <- attributes(out)
  }
  if ("out_txt" %in% out_to_read) {
    out_to_read <- setdiff(out_to_read, "out_txt")
    # Analyze the out_detail.txt file
    out_txt <- out
    # Removing known text
    out_txt <- trimws(out_txt)
    out_txt <- out_txt[out_txt != ""]
    out_txt <- out_txt[!grepl("^(FSITE|FSUBA|FWPM|FWIND|FCROP|FTILL|FPEST|FFERT|FSOIL|FOPSC|FTR55|FPARM|FMLRN|FPRNT|FHERD|FWLST|FPSOD|FRFDT) +[[:alnum:].]+$", out_txt)]
    out_txt <- out_txt[!grepl("^RUN # = +[0-9]+ +SUBAREA FILE = [[:alnum:].]+$", out_txt)]
    out_txt <- out_txt[!grepl("^YEAR +[0-9]+ OF +[0-9]+$", out_txt)]
    out_txt <- setdiff(out_txt, c("Loading required package: data.table",
                                  "Reading  ./fk.SAD"))
    
    attributes(out_txt) <- NULL
    rprt[["out_txt"]] <- out_txt
  }
  rprt0 <- lapply(out_to_read, function(x) {message("Reading", x); read_out_files(x, folder=folder)})
  names(rprt0) <- gsub("^\\.", "", tolower(out_to_read))
  rprt <- c(rprt, rprt0)
  return(rprt)
}
