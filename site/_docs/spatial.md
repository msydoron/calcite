---
layout: docs
title: Spatial
permalink: /docs/spatial.html
---
<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

Calcite is [aiming](https://issues.apache.org/jira/browse/CALCITE-1968) to implement
OpenGIS Simple Features Implementation Specification for SQL,
[version 1.2.1](http://www.opengeospatial.org/standards/sfs),
a standard implemented by spatial databases such as
[PostGIS](http://postgis.net/)
and [H2GIS](http://www.h2gis.org/).

We also aim to add optimizer support for
[spatial indexes](https://issues.apache.org/jira/browse/CALCITE-1861)
and other forms of query optimization.

* TOC
{:toc}

## Introduction

A spatial database is a database that is optimized for storing and query data
that represents objects defined in a geometric space.

Calcite's support for spatial data includes:

* A [GEOMETRY](reference.html#data-types) data type and
  [sub-types](reference.html#spatial-types) including `POINT`, `LINESTRING`
  and `POLYGON`
* [Spatial functions](reference.html#spatial-functions) (prefixed `ST_`;
  we have implemented about 35 of the 150 in the OpenGIS specification)

and will at some point also include query rewrites to use spatial indexes.

## Query rewrites

One class of rewrites uses Hilbert space-filling curves. Suppose that a table
has columns `x` and `y` denoting the position of a point and also a column `h`
denoting the distance of that point along a curve. Then a predicate involving
distance of (x, y) from a fixed point can be translated into a predicate
involving ranges of h.

Suppose we have table

{% highlight sql %}
CREATE TABLE Restaurants (
  INT id NOT NULL PRIMARY KEY,
  VARCHAR(30) name,
  VARCHAR(20) cuisine,
  INT x NOT NULL,
  INT y NOT NULL,
  INT h  NOT NULL CHECK (h = ST_Hilbert(x, y)))
SORT KEY (h);
{% endhighlight %}

(The DDL syntax is imaginary, but illustrates the fact that `h` is the position
on the Hilbert curve of point (`x`, `y`), and also that the table is sorted on
`h`.)

The query

{% highlight sql %}
SELECT *
FROM Restaurants
WHERE ST_DWithin(ST_Point(x, y), ST_Point(10.0, 20.0), 6)
{% endhighlight %}

can be rewritten to

{% highlight sql %}
SELECT *
FROM Restaurants
WHERE (h BETWEEN 36496 AND 36520
    OR h BETWEEN 36456 AND 36464
    OR h BETWEEN 33252 AND 33254
    OR h BETWEEN 33236 AND 33244
    OR h BETWEEN 33164 AND 33176
    OR h BETWEEN 33092 AND 33100
    OR h BETWEEN 33055 AND 33080
    OR h BETWEEN 33050 AND 33053
    OR h BETWEEN 33033 AND 33035)
AND ST_DWithin(ST_Point(x, y), ST_Point(10.0, 20.0), 6)
{% endhighlight %}

The rewritten query contains a collection of ranges on `h` followed by the
original `ST_DWithin` predicate. The range predicates are evaluated first and
are very fast because the table is sorted on `h`.

Here is the full set of supported rewrites

| Before | Rewritten to | Exact?
|:------ |: ----------- |: ---
| ST_Contains(&#8203;ST_Rectangle(&#8203;X - D, X + D, Y - D, Y + D), ST_Point(a, b))) | (h BETWEEN C1 AND C2<br/>OR ...<br/>OR h BETWEEN C7 AND C8) | Yes
| ST_DWithin(&#8203;ST_Point(a, b), ST_Point(X, Y), D))<br/><br/>ST_Distance(&#8203;ST_Point(a, b), ST_Point(X, Y))) <= D<br/><br/>ST_Contains(&#8203;ST_Buffer(ST_Point(a, b), D), ST_Point(X, Y)) | (h BETWEEN C1 AND C2<br/>OR ...<br/>OR h BETWEEN C7 AND C8)<br/>AND p | No

In the above, `a` and `b` are variables, `X`, `Y` and `D` are constants.

## Acknowledgements

Calcite's OpenGIS implementation uses the
[Esri geometry API](https://github.com/Esri/geometry-api-java). Thanks for the
help we received from their community.

While developing this feature, we made extensive use of the
PostGIS documentation and tests,
and the H2GIS documentation, and consulted both as reference implementations
when the specification wasn't clear. Thank you to these awesome projects.
