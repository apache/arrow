# transmute() defuses dots arguments (ARROW-13262)

    Code
      tbl %>% Table$create() %>% transmute(a = stringr::str_c(padded_strings,
        padded_strings), b = stringr::str_squish(a)) %>% collect()
    Condition
      Warning:
      In stringr::str_squish(a): 
      i Expression not supported in Arrow
      > Pulling data into R
    Output
      # A tibble: 10 x 2
         a                                            b    
         <chr>                                        <chr>
       1 " a  a "                                     a a  
       2 "  b    b  "                                 b b  
       3 "   c      c   "                             c c  
       4 "    d        d    "                         d d  
       5 "     e          e     "                     e e  
       6 "      f            f      "                 f f  
       7 "       g              g       "             g g  
       8 "        h                h        "         h h  
       9 "         i                  i         "     i i  
      10 "          j                    j          " j j  

