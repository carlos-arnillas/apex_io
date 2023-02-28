# apex_io
APEX IO provides a way to read APEX output files. Notice that the code is not optimized to read the output quickly, but to work with the very different outputs created by APEX.
Not all formats are yet supported. If the format is not supported, the code will indicate it.

To use:

Load the codes:
```
source("../../apex_r/codes/apex_functions_2.R")
```

Read the .SAD files... that are present in the working directory
```
sad <- read_out_files(".SAD", folder=".")
```
