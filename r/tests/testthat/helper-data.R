# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

example_data <- tibble::tibble(
  int = c(1:3, NA_integer_, 5:10),
  dbl = c(1:8, NA, 10) + .1,
  dbl2 = rep(5, 10),
  lgl = sample(c(TRUE, FALSE, NA), 10, replace = TRUE),
  false = logical(10),
  chr = letters[c(1:5, NA, 7:10)],
  fct = factor(letters[c(1:4, NA, NA, 7:10)])
)

example_with_metadata <- tibble::tibble(
  a = structure("one", class = "special_string"),
  b = 2,
  c = tibble::tibble(
    c1 = structure("inner", extra_attr = "something"),
    c2 = 4,
    c3 = 50
  ),
  d = "four"
)

attr(example_with_metadata, "top_level") <- list(
  field_one = 12,
  field_two = "more stuff"
)

haven_data <- tibble::tibble(
  num = structure(c(5.1, 4.9),
    format.spss = "F8.2"
  ),
  cat_int = structure(c(3, 1),
    format.spss = "F8.0",
    labels = c(first = 1, second = 2, third = 3),
    class = c("haven_labelled", "vctrs_vctr", "double")
  ),
  cat_chr = structure(c("B", "B"),
    labels = c(Alpha = "A", Beta = "B"),
    class = c("haven_labelled", "vctrs_vctr", "character")
  )
)

example_with_times <- tibble::tibble(
  date = Sys.Date() + 1:10,
  posixct = lubridate::ymd_hms("2018-10-07 19:04:05") + 1:10,
  posixct_tz = lubridate::ymd_hms("2018-10-07 19:04:05", tz = "US/Eastern") + 1:10,
  posixlt = as.POSIXlt(lubridate::ymd_hms("2018-10-07 19:04:05") + 1:10),
  posixlt_tz = as.POSIXlt(lubridate::ymd_hms("2018-10-07 19:04:05", tz = "US/Eastern") + 1:10)
)

verses <- list(
  # Since we tend to test with dataframes with 10 rows, here are verses from
  # "Milonga del moro judío", by Jorge Drexler. They are décimas, 10-line
  # poems with a particular meter and rhyme scheme.
  # (They also have non-ASCII characters, which is nice for testing)
  c(
    "Por cada muro, un lamento",
    "En Jerusalén la dorada",
    "Y mil vidas malgastadas",
    "Por cada mandamiento",
    "Yo soy polvo de tu viento",
    "Y aunque sangro de tu herida",
    "Y cada piedra querida",
    "Guarda mi amor más profundo",
    "No hay una piedra en el mundo",
    "Que valga lo que una vida"
  ),
  c(
    "No hay muerto que no me duela",
    "No hay un bando ganador",
    "No hay nada más que dolor",
    "Y otra vida que se vuela",
    "La guerra es muy mala escuela",
    "No importa el disfraz que viste",
    "Perdonen que no me aliste",
    "Bajo ninguna bandera",
    "Vale más cualquier quimera",
    "Que un trozo de tela triste"
  ),
  c(
    "Y a nadie le di permiso",
    "Para matar en mi nombre",
    "Un hombre no es más que un hombre",
    "Y si hay Dios, así lo quiso",
    "El mismo suelo que piso",
    "Seguirá, yo me habré ido",
    "Rumbo también del olvido",
    "No hay doctrina que no vaya",
    "Y no hay pueblo que no se haya",
    "Creído el pueblo elegido"
  )
)

make_big_string <- function() {
  # This creates a character vector that would exceed the capacity of BinaryArray
  rep(purrr::map_chr(2047:2050, ~paste(sample(letters, ., replace = TRUE), collapse = "")), 2^18)
}

make_random_string_of_size <- function(size = 1) {
  purrr::map_chr(1000*size, ~paste(sample(letters, ., replace = TRUE), collapse = ""))
}

make_string_of_size <- function(size = 1) {
  paste(rep(letters, length = 1000*size), collapse = "")
}

example_with_extra_metadata <- example_with_metadata
attributes(example_with_extra_metadata$b) <- list(lots = rep(make_string_of_size(1), 100))

example_with_logical_factors <- tibble::tibble(
  starting_a_fight = factor(c(FALSE, TRUE, TRUE, TRUE)),
  consoling_a_child = factor(c(TRUE, FALSE, TRUE, TRUE)),
  petting_a_dog = factor(c(TRUE, TRUE, FALSE, TRUE)),
  saying = c(
    "shhhhh, it's ok",
    "you wanna go outside?",
    "you want your mommy?",
    "hey buddy"
  )
)

# The values in each column of this tibble are in ascending order. There are
# some ties, so tests should use two or more columns to ensure deterministic
# sort order. The Arrow C++ library orders strings lexicographically as byte
# strings. The order of a string array sorted by Arrow will not match the order
# of an equivalent character vector sorted by R unless you set the R collation
# locale to "C" by running:
#   Sys.setlocale("LC_COLLATE", "C")
# These test scripts set that, but if you are running individual tests you might
# need to set it manually. When finished, you can restore the default
# collation locale by running:
#   Sys.setlocale("LC_COLLATE")
# In the future, the string collation locale used by the Arrow C++ library might
# be configurable (ARROW-12046).
example_data_for_sorting <- tibble::tibble(
  int = c(-.Machine$integer.max, -101L, -100L, 0L, 0L, 1L, 100L, 1000L, .Machine$integer.max, NA_integer_),
  dbl = c(-Inf, -.Machine$double.xmax, -.Machine$double.xmin, 0, .Machine$double.xmin, pi, .Machine$double.xmax, Inf, NaN, NA_real_),
  chr = c("", "", "\"", "&", "ABC", "NULL", "a", "abc", "zzz", NA_character_),
  lgl = c(rep(FALSE, 4L), rep(TRUE, 5L), NA),
  dttm = lubridate::ymd_hms(c(
    "0000-01-01 00:00:00",
    "1919-05-29 13:08:55",
    "1955-06-20 04:10:42",
    "1973-06-30 11:38:41",
    "1987-03-29 12:49:47",
    "1991-06-11 19:07:01",
    NA_character_,
    "2017-08-21 18:26:40",
    "2017-08-21 18:26:40",
    "9999-12-31 23:59:59"
  )),
  grp = c(rep("A", 5), rep("B", 5))
)
