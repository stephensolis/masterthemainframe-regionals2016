masterthemainframe-regionals2016
================================

This is the analysis code I wrote for the last challenge of the 2016 IBM Master
the Mainframe Regionals.

Files
-----

* The queries in IBM DB2 SQL (this is what I submitted) are in
`analysis.db2.sql`. You can run these with
`original-data/CARDUSR_CLIENT_INFO.sql.zip` and
`original-data/CARDUSR_SPPAYTB.sql.zip` from
[stephensolis/masterthemainframe-wc2016](https://github.com/stephensolis/masterthemainframe-wc2016).
	* The same queries for Postgres are in `analysis.postgres.sql`.
* The queries in [Spark](https://spark.apache.org/) (both the DataFrame API and
Spark SQL) are in `analysis.spark.scala`. You can run these with
`analysis/clients.orc` and `analysis/transactions.orc` from
[stephensolis/masterthemainframe-wc2016](https://github.com/stephensolis/masterthemainframe-wc2016).

License ![License](http://img.shields.io/:license-mit-blue.svg)
-------

    The MIT License (MIT)

    Copyright (c) 2016 Stephen

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
