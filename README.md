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
sad1 <- read_out_files(".SAD", folder=".")
```
If all fields are recognized, the table sad1 will have all the information properly formatted. However, if there is a field that the function does not recognize, it will present a small table showing the fields that were recognized and those that were not. 


Sources used to document the database_structure2.xlsx file:
- Steglich, Osorio, Doro, Jeong, and Williams. 2018. Agricultural Policy/Environmental eXtender Model User’s Manual Version 1501.
- ArcAPEX database
