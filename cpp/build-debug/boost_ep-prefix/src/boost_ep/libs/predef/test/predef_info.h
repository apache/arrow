/*
Copyright Rene Rivera 2011-2015
Distributed under the Boost Software License, Version 1.0.
(See accompanying file LICENSE_1_0.txt or copy at
http://www.boost.org/LICENSE_1_0.txt)
*/
#include <boost/predef/detail/test_def.h>

int main()
{
    unsigned x = 0;
    create_predef_entries();
    qsort(generated_predef_info,generated_predef_info_count,
        sizeof(predef_info),predef_info_compare);
    /*
    for (x = 0; x < generated_predef_info_count; ++x)
    {
        printf("%s: %d\n", generated_predef_info[x].name, generated_predef_info[x].value);
    }
    */
    puts("** Detected **");
    for (x = 0; x < generated_predef_info_count; ++x)
    {
        if (generated_predef_info[x].value > 0)
            printf("%s = %u (%u,%u,%u) | %s\n",
                generated_predef_info[x].name,
                generated_predef_info[x].value,
                (generated_predef_info[x].value/10000000)%100,
                (generated_predef_info[x].value/100000)%100,
                (generated_predef_info[x].value)%100000,
                generated_predef_info[x].description);
    }
    puts("** Not Detected **");
    for (x = 0; x < generated_predef_info_count; ++x)
    {
        if (generated_predef_info[x].value == 0)
            printf("%s = %u | %s\n",
                generated_predef_info[x].name,
                generated_predef_info[x].value,
                generated_predef_info[x].description);
    }
    if (generated_predef_info_count > 0)
        return 0;
    else
        return 1;
}
