package org.apache.carbondata.examples.performance

import org.apache.spark.sql.SparkSession

object QueryUtil {

  // ------------------------------------------ TPC-H QUERIES
  // -----------------------------------------------------

  val queryList: List[String] = List(
    "select\n        l_returnflag,\n        l_linestatus,\n        sum(l_quantity) as " +
      "sum_qty,\n        sum(l_extendedprice) as sum_base_price,\n        sum(l_extendedprice" +
      " * (1 - l_discount)) as sum_disc_price,\n        sum(l_extendedprice * (1 - " +
      "l_discount) * (1 + l_tax)) as sum_charge,\n        avg(l_quantity) as avg_qty,\n      " +
      "  avg(l_extendedprice) as avg_price,\n        avg(l_discount) as avg_disc,\n        " +
      "count(*) as count_order\nfrom\n        lineitem\nwhere\n        l_shipdate <= " +
      "cast('1998-09-16' as timestamp)\ngroup by\n        l_returnflag,\n        l_linestatus\norder by\n       " +
      " l_returnflag,\n        l_linestatus".toString(),
    "select\n        l_orderkey,\n        sum(l_extendedprice * (1 - l_discount)) as " +
      "revenue,\n        o_orderdate,\n        o_shippriority\nfrom\n        customer,\n     " +
      "   orders,\n        lineitem\nwhere\n        c_mktsegment = 'BUILDING'\n        and " +
      "c_custkey = o_custkey\n        and l_orderkey = o_orderkey\n        and o_orderdate < " +
      " cast('1995-03-22' as timestamp)\n        and l_shipdate > cast('1995-03-22' as timestamp)\ngroup by\n        l_orderkey,\n  " +
      "      o_orderdate,\n        o_shippriority\norder by\n        revenue desc,\n        " +
      "o_orderdate\nlimit 10".toString(),
    "select\n        o_orderpriority,\n        count(*) as order_count\nfrom\n        " +
      "orders as o\nwhere\n        o_orderdate >= cast('1996-05-01' as timestamp)\n        and o_orderdate < " +
      "cast('1996-08-01' as timestamp)\n        and exists (\n                select\n                        " +
      "*\n                from\n                        lineitem\n                where\n    " +
      "                    l_orderkey = o.o_orderkey\n                        and " +
      "l_commitdate < l_receiptdate\n        )\ngroup by\n        o_orderpriority\norder by\n" +
      "        o_orderpriority".toString(),
    "select\n        n_name,\n        sum(l_extendedprice * (1 - l_discount)) as " +
      "revenue\nfrom\n        customer,\n        orders,\n        lineitem,\n        " +
      "supplier,\n        nation,\n        region\nwhere\n        c_custkey = o_custkey\n    " +
      "    and l_orderkey = o_orderkey\n        and l_suppkey = s_suppkey\n        and " +
      "c_nationkey = s_nationkey\n        and s_nationkey = n_nationkey\n        and " +
      "n_regionkey = r_regionkey\n        and r_name = 'AFRICA'\n        and o_orderdate >= " +
      "cast('1993-01-01' as timestamp)\n        and o_orderdate < cast('1994-01-01' as timestamp)" +
      "\ngroup by\n        n_name\norder " +
      "by\n        revenue desc".toString(),
    "select\n        o_orderpriority,\n        count(*) as order_count\nfrom\n        " +
      "orders as o\nwhere\n        o_orderdate >= cast('1996-05-01' as timestamp)\n        and o_orderdate < " +
      "cast('1996-08-01' as timestamp)\n        and exists (\n                select\n                        " +
      "*\n                from\n                        lineitem\n                where\n    " +
      "                    l_orderkey = o.o_orderkey\n                        and " +
      "l_commitdate < l_receiptdate\n        )\ngroup by\n        o_orderpriority\norder by\n" +
      "        o_orderpriority".toString(),
    "select\n        n_name,\n        sum(l_extendedprice * (1 - l_discount)) as " +
      "revenue\nfrom\n        customer,\n        orders,\n        lineitem,\n        " +
      "supplier,\n        nation,\n        region\nwhere\n        c_custkey = o_custkey\n    " +
      "    and l_orderkey = o_orderkey\n        and l_suppkey = s_suppkey\n        and " +
      "c_nationkey = s_nationkey\n        and s_nationkey = n_nationkey\n        and " +
      "n_regionkey = r_regionkey\n        and r_name = 'AFRICA'\n        and o_orderdate >= " +
      "cast('1993-01-01' as timestamp)\n        and o_orderdate < cast('1994-01-01' as timestamp) \ngroup by\n        n_name\norder " +
      "by\n        revenue desc".toString(),
    "select\n        sum(l_extendedprice * l_discount) as revenue\nfrom\n        " +
      "lineitem\nwhere\n        l_shipdate >= cast('1993-01-01' as timestamp)\n        and l_shipdate < " +
      "cast('1994-01-01' as timestamp) \n        and l_discount between 0.06 - 0.01 and 0.06 + 0.01\n        and " +
      "l_quantity < 25".toString(),
    "select\n        supp_nation,\n        cust_nation,\n        l_year,\n        sum" +
      "(volume) as revenue\nfrom\n        (\n                select\n                        " +
      "n1.n_name as supp_nation,\n                        n2.n_name as cust_nation,\n        " +
      "                year(l_shipdate) as l_year,\n                        l_extendedprice *" +
      " (1 - l_discount) as volume\n                from\n                        supplier,\n" +
      "                        lineitem,\n                        orders,\n                  " +
      "      customer,\n                        nation n1,\n                        nation " +
      "n2\n                where\n                        s_suppkey = l_suppkey\n            " +
      "            and o_orderkey = l_orderkey\n                        and c_custkey = " +
      "o_custkey\n                        and s_nationkey = n1.n_nationkey\n                 " +
      "       and c_nationkey = n2.n_nationkey\n                        and (\n              " +
      "                  (n1.n_name = 'KENYA' and n2.n_name = 'PERU')\n                      " +
      "          or (n1.n_name = 'PERU' and n2.n_name = 'KENYA')\n                        )\n" +
      "                        and l_shipdate between cast('1995-01-01' as timestamp) and cast('1996-12-31' as timestamp)\n        " +
      ") as shipping\ngroup by\n        supp_nation,\n        cust_nation,\n        " +
      "l_year\norder by\n        supp_nation,\n        cust_nation,\n        l_year".toString(),
    "select\n        o_year,\n        sum(case\n                when nation = 'PERU' then " +
      "volume\n                else 0\n        end) / sum(volume) as mkt_share\nfrom\n       " +
      " (\n                select\n                        year(o_orderdate) as o_year,\n    " +
      "                    l_extendedprice * (1 - l_discount) as volume,\n                   " +
      "     n2.n_name as nation\n                from\n                        part,\n       " +
      "                 supplier,\n                        lineitem,\n                       " +
      " orders,\n                        customer,\n                        nation n1,\n     " +
      "                   nation n2,\n                        region\n                where\n" +
      "                        p_partkey = l_partkey\n                        and s_suppkey =" +
      " l_suppkey\n                        and l_orderkey = o_orderkey\n                     " +
      "   and o_custkey = c_custkey\n                        and c_nationkey = n1" +
      ".n_nationkey\n                        and n1.n_regionkey = r_regionkey\n              " +
      "          and r_name = 'AMERICA'\n                        and s_nationkey = n2" +
      ".n_nationkey\n                        and o_orderdate between cast('1995-01-01' as timestamp) and " +
      " cast('1996-12-31' as timestamp)\n                        and p_type = 'ECONOMY BURNISHED NICKEL'\n       " +
      " ) as all_nations\ngroup by\n        o_year\norder by\n        o_year".toString(),
     "select\n        c_custkey,\n        c_name,\n        sum(l_extendedprice * (1 - " +
      "l_discount)) as revenue,\n        c_acctbal,\n        n_name,\n        c_address,\n   " +
      "     c_phone,\n        c_comment\nfrom\n        customer,\n        orders,\n        " +
      "lineitem,\n        nation\nwhere\n        c_custkey = o_custkey\n        and " +
      "l_orderkey = o_orderkey\n        and o_orderdate >= cast('1993-07-01' as timestamp)\n        and " +
      "o_orderdate < cast('1993-10-01' as timestamp)\n        and l_returnflag = 'R'\n        and c_nationkey = " +
      "n_nationkey\ngroup by\n        c_custkey,\n        c_name,\n        c_acctbal,\n      " +
      "  c_phone,\n        n_name,\n        c_address,\n        c_comment\norder by\n        " +
      "revenue desc\nlimit 20".toString(),
    "select\n        l_shipmode,\n        sum(case\n                when o_orderpriority = " +
      "'1-URGENT'\n                        or o_orderpriority = '2-HIGH'\n                   " +
      "     then 1\n                else 0\n        end) as high_line_count,\n        sum" +
      "(case\n                when o_orderpriority <> '1-URGENT'\n                        and" +
      " o_orderpriority <> '2-HIGH'\n                        then 1\n                else 0\n" +
      "        end) as low_line_count\nfrom\n        orders,\n        lineitem\nwhere\n      " +
      "  o_orderkey = l_orderkey\n        and l_shipmode in ('REG AIR', 'MAIL')\n        and " +
      "l_commitdate < l_receiptdate\n        and l_shipdate < l_commitdate\n        and " +
      "l_receiptdate >= cast('1995-01-01' as timestamp) \n        and l_receiptdate < cast('1996-01-01' as timestamp) \ngroup by\n   " +
      "     l_shipmode\norder by\n        l_shipmode".toString(),
    "select\n        c_count,\n        count(*) as custdist\nfrom\n        (\n             " +
      "   select\n                        c_custkey,\n                        count" +
      "(o_orderkey) as c_count\n                from\n                        customer left " +
      "outer join orders on\n                                c_custkey = o_custkey\n         " +
      "                       and o_comment not like '%unusual%accounts%'\n                " +
      "group by\n                        c_custkey\n        ) c_orders\ngroup by\n        " +
      "c_count\norder by\n        custdist desc,\n        c_count desc".toString(),
    "select\n        100.00 * sum(case\n                when p_type like 'PROMO%'\n        " +
      "                then l_extendedprice * (1 - l_discount)\n                else 0\n     " +
      "   end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue\nfrom\n        " +
      "lineitem,\n        part\nwhere\n        l_partkey = p_partkey\n        and l_shipdate " +
      ">= cast('1995-08-01' as timestamp) \n        and l_shipdate < cast('1995-09-01' as timestamp)".toString(),
    "select\n        p_brand,\n        p_type,\n        p_size,\n        count(distinct " +
      "ps_suppkey) as supplier_cnt\nfrom\n        partsupp,\n        part\nwhere\n        " +
      "p_partkey = ps_partkey\n        and p_brand <> 'Brand#34'\n        and p_type not like" +
      " 'ECONOMY BRUSHED%'\n        and p_size in (22, 14, 27, 49, 21, 33, 35, 28)\n        " +
      "and partsupp.ps_suppkey not in (\n                select\n                        " +
      "s_suppkey\n                from\n                        supplier\n                " +
      "where\n                        s_comment like '%Customer%Complaints%'\n        )" +
      "\ngroup by\n        p_brand,\n        p_type,\n        p_size\norder by\n        " +
      "supplier_cnt desc,\n        p_brand,\n        p_type,\n        p_size".toString(),
    "with q17_part as (\n  select p_partkey from part where  \n  p_brand = 'Brand#23'\n  " +
      "and p_container = 'MED BOX'\n),\nq17_avg as (\n  select l_partkey as t_partkey, 0.2 * " +
      "avg(l_quantity) as t_avg_quantity\n  from lineitem \n  where l_partkey IN (select " +
      "p_partkey from q17_part)\n  group by l_partkey\n),\nq17_price as (\n  select\n  " +
      "l_quantity,\n  l_partkey,\n  l_extendedprice\n  from\n  lineitem\n  where\n  l_partkey" +
      " IN (select p_partkey from q17_part)\n)\nselect cast(sum(l_extendedprice) / 7.0 as " +
      "decimal(32,2)) as avg_yearly\nfrom q17_avg, q17_price\nwhere \nt_partkey = l_partkey " +
      "and l_quantity < t_avg_quantity".toString(),
    "-- explain formatted \nwith tmp1 as (\n    select p_partkey from part where p_name " +
      "like 'forest%'\n),\ntmp2 as (\n    select s_name, s_address, s_suppkey\n    from " +
      "supplier, nation\n    where s_nationkey = n_nationkey\n    and n_name = 'CANADA'\n)," +
      "\ntmp3 as (\n    select l_partkey, 0.5 * sum(l_quantity) as sum_quantity, l_suppkey\n " +
      "   from lineitem, tmp2\n    where l_shipdate >= cast('1994-01-01' as timestamp) and l_shipdate <= " +
      "cast('1995-01-01' as timestamp)\n    and l_suppkey = s_suppkey \n    group by l_partkey, l_suppkey\n)," +
      "\ntmp4 as (\n    select ps_partkey, ps_suppkey, ps_availqty\n    from partsupp \n    " +
      "where ps_partkey IN (select p_partkey from tmp1)\n),\ntmp5 as (\nselect\n    " +
      "ps_suppkey\nfrom\n    tmp4, tmp3\nwhere\n    ps_partkey = l_partkey\n    and " +
      "ps_suppkey = l_suppkey\n    and ps_availqty > sum_quantity\n)\nselect\n    s_name,\n  " +
      "  s_address\nfrom\n    supplier\nwhere\n    s_suppkey IN (select ps_suppkey from tmp5)" +
      "\norder by s_name".toString()
/*    """select
      |        l_returnflag,
      |        l_linestatus,
      |        sum(l_quantity) as sum_qty,
      |        sum(l_extendedprice) as sum_base_price,
      |        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
      |        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
      |        avg(l_quantity) as avg_qty,
      |        avg(l_extendedprice) as avg_price,
      |        avg(l_discount) as avg_disc,
      |        count(*) as count_order
      |from
      |        lineitem
      |where
      |        l_shipdate <= '1998-09-16'
      |group by
      |        l_returnflag,
      |        l_linestatus
      |order by
      |        l_returnflag,
      |        l_linestatus""".stripMargin,

    """select
      |        l_orderkey,
      |        sum(l_extendedprice * (1 - l_discount)) as revenue,
      |        o_orderdate,
      |        o_shippriority
      |from
      |        customer,
      |        orders,
      |        lineitem
      |where
      |        c_mktsegment = 'BUILDING'
      |        and c_custkey = o_custkey
      |        and l_orderkey = o_orderkey
      |        and o_orderdate < '1995-03-22'
      |        and l_shipdate > '1995-03-22'
      |group by
      |        l_orderkey,
      |        o_orderdate,
      |        o_shippriority
      |order by
      |        revenue desc,
      |        o_orderdate
      |limit 10""".stripMargin,

    """select
      |        o_orderpriority,
      |        count(*) as order_count
      |from
      |        orders as o
      |where
      |        o_orderdate >= '1996-05-01'
      |        and o_orderdate < '1996-08-01'
      |        and exists (
      |                select
      |                        *
      |                from
      |                        lineitem
      |                where
      |                        l_orderkey = o.o_orderkey
      |                        and l_commitdate < l_receiptdate
      |        )
      |group by
      |        o_orderpriority
      |order by
      |        o_orderpriority""".stripMargin,

    """select
      |        n_name,
      |        sum(l_extendedprice * (1 - l_discount)) as revenue
      |from
      |        customer,
      |        orders,
      |        lineitem,
      |        supplier,
      |        nation,
      |        region
      |where
      |        c_custkey = o_custkey
      |        and l_orderkey = o_orderkey
      |        and l_suppkey = s_suppkey
      |        and c_nationkey = s_nationkey
      |        and s_nationkey = n_nationkey
      |        and n_regionkey = r_regionkey
      |        and r_name = 'AFRICA'
      |        and o_orderdate >= '1993-01-01'
      |        and o_orderdate < '1994-01-01'
      |group by
      |        n_name
      |order by
      |        revenue desc""".stripMargin,

    """select
      |        sum(l_extendedprice * l_discount) as revenue
      |from
      |        lineitem
      |where
      |        l_shipdate >= '1993-01-01'
      |        and l_shipdate < '1994-01-01'
      |        and l_discount between 0.06 - 0.01 and 0.06 + 0.01
      |        and l_quantity < 25""".stripMargin,

    """select
      |        supp_nation,
      |        cust_nation,
      |        l_year,
      |        sum(volume) as revenue
      |from
      |        (
      |                select
      |                        n1.n_name as supp_nation,
      |                        n2.n_name as cust_nation,
      |                        year(l_shipdate) as l_year,
      |                        l_extendedprice * (1 - l_discount) as volume
      |                from
      |                        supplier,
      |                        lineitem,
      |                        orders,
      |                        customer,
      |                        nation n1,
      |                        nation n2
      |                where
      |                        s_suppkey = l_suppkey
      |                        and o_orderkey = l_orderkey
      |                        and c_custkey = o_custkey
      |                        and s_nationkey = n1.n_nationkey
      |                        and c_nationkey = n2.n_nationkey
      |                        and (
      |                                (n1.n_name = 'KENYA' and n2.n_name = 'PERU')
      |                                or (n1.n_name = 'PERU' and n2.n_name = 'KENYA')
      |                        )
      |                        and l_shipdate between '1995-01-01' and '1996-12-31'
      |        ) as shipping
      |group by
      |        supp_nation,
      |        cust_nation,
      |        l_year
      |order by
      |        supp_nation,
      |        cust_nation,
      |        l_year""".stripMargin,

    """select
      |        o_year,
      |        sum(case
      |                when nation = 'PERU' then volume
      |                else 0
      |        end) / sum(volume) as mkt_share
      |from
      |        (
      |                select
      |                        year(o_orderdate) as o_year,
      |                        l_extendedprice * (1 - l_discount) as volume,
      |                        n2.n_name as nation
      |                from
      |                        part,
      |                        supplier,
      |                        lineitem,
      |                        orders,
      |                        customer,
      |                        nation n1,
      |                        nation n2,
      |                        region
      |                where
      |                        p_partkey = l_partkey
      |                        and s_suppkey = l_suppkey
      |                        and l_orderkey = o_orderkey
      |                        and o_custkey = c_custkey
      |                        and c_nationkey = n1.n_nationkey
      |                        and n1.n_regionkey = r_regionkey
      |                        and r_name = 'AMERICA'
      |                        and s_nationkey = n2.n_nationkey
      |                        and o_orderdate between '1995-01-01' and '1996-12-31'
      |                        and p_type = 'ECONOMY BURNISHED NICKEL'
      |        ) as all_nations
      |group by
      |        o_year
      |order by
      |        o_year""".stripMargin,

    """select
      |        nation,
      |        o_year,
      |        sum(amount) as sum_profit
      |from
      |        (
      |                select
      |                        n_name as nation,
      |                        year(o_orderdate) as o_year,
      |                        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity
      |                        as amount
      |                from
      |                        part,
      |                        supplier,
      |                        lineitem,
      |                        partsupp,
      |                        orders,
      |                        nation
      |                where
      |                        s_suppkey = l_suppkey
      |                        and ps_suppkey = l_suppkey
      |                        and ps_partkey = l_partkey
      |                        and p_partkey = l_partkey
      |                        and o_orderkey = l_orderkey
      |                        and s_nationkey = n_nationkey
      |                        and p_name like '%plum%'
      |        ) as profit
      |group by
      |        nation,
      |        o_year
      |order by
      |        nation,
      |        o_year desc""".stripMargin,

    """select
      |        c_custkey,
      |        c_name,
      |        sum(l_extendedprice * (1 - l_discount)) as revenue,
      |        c_acctbal,
      |        n_name,
      |        c_address,
      |        c_phone,
      |        c_comment
      |from
      |        customer,
      |        orders,
      |        lineitem,
      |        nation
      |where
      |        c_custkey = o_custkey
      |        and l_orderkey = o_orderkey
      |        and o_orderdate >= '1993-07-01'
      |        and o_orderdate < '1993-10-01'
      |        and l_returnflag = 'R'
      |        and c_nationkey = n_nationkey
      |group by
      |        c_custkey,
      |        c_name,
      |        c_acctbal,
      |        c_phone,
      |        n_name,
      |        c_address,
      |        c_comment
      |order by
      |        revenue desc
      |limit 20""".stripMargin,

    """select
      |        l_shipmode,
      |        sum(case
      |                when o_orderpriority = '1-URGENT'
      |                        or o_orderpriority = '2-HIGH'
      |                        then 1
      |                else 0
      |        end) as high_line_count,
      |        sum(case
      |                when o_orderpriority <> '1-URGENT'
      |                        and o_orderpriority <> '2-HIGH'
      |                        then 1
      |                else 0
      |        end) as low_line_count
      |from
      |        orders,
      |        lineitem
      |where
      |        o_orderkey = l_orderkey
      |        and l_shipmode in ('REG AIR', 'MAIL')
      |        and l_commitdate < l_receiptdate
      |        and l_shipdate < l_commitdate
      |        and l_receiptdate >= '1995-01-01'
      |        and l_receiptdate < '1996-01-01'
      |group by
      |        l_shipmode
      |order by
      |        l_shipmode""".stripMargin,

    """select
      |        c_count,
      |        count(*) as custdist
      |from
      |        (
      |                select
      |                        c_custkey,
      |                        count(o_orderkey) as c_count
      |                from
      |                        customer left outer join orders on
      |                                c_custkey = o_custkey
      |                                and o_comment not like '%unusual%accounts%'
      |                group by
      |                        c_custkey
      |        ) c_orders
      |group by
      |        c_count
      |order by
      |        custdist desc,
      |        c_count desc""".stripMargin,

    """select
      |        100.00 * sum(case
      |                when p_type like 'PROMO%'
      |                        then l_extendedprice * (1 - l_discount)
      |                else 0
      |        end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
      |from
      |        lineitem,
      |        part
      |where
      |        l_partkey = p_partkey
      |        and l_shipdate >= '1995-08-01'
      |        and l_shipdate < '1995-09-01'""".stripMargin,

    """select
      |        p_brand,
      |        p_type,
      |        p_size,
      |        count(distinct ps_suppkey) as supplier_cnt
      |from
      |        partsupp,
      |        part
      |where
      |        p_partkey = ps_partkey
      |        and p_brand <> 'Brand#34'
      |        and p_type not like 'ECONOMY BRUSHED%'
      |        and p_size in (22, 14, 27, 49, 21, 33, 35, 28)
      |        and partsupp.ps_suppkey not in (
      |                select
      |                        s_suppkey
      |                from
      |                        supplier
      |                where
      |                        s_comment like '%Customer%Complaints%'
      |        )
      |group by
      |        p_brand,
      |        p_type,
      |        p_size
      |order by
      |        supplier_cnt desc,
      |        p_brand,
      |        p_type,
      |        p_size""".stripMargin,

    """with q17_part as (
      |  select p_partkey from part where
      |  p_brand = 'Brand#23'
      |  and p_container = 'MED BOX'
      |),
      |q17_avg as (
      |  select l_partkey as t_partkey, 0.2 * avg(l_quantity) as t_avg_quantity
      |  from lineitem
      |  where l_partkey IN (select p_partkey from q17_part)
      |  group by l_partkey
      |),
      |q17_price as (
      |  select
      |  l_quantity,
      |  l_partkey,
      |  l_extendedprice
      |  from
      |  lineitem
      |  where
      |  l_partkey IN (select p_partkey from q17_part)
      |)
      |select cast(sum(l_extendedprice) / 7.0 as decimal(32,2)) as avg_yearly
      |from q17_avg, q17_price
      |where
      |t_partkey = l_partkey and l_quantity < t_avg_quantity""".stripMargin,

    """select
      |        sum(l_extendedprice* (1 - l_discount)) as revenue
      |from
      |        lineitem,
      |        part
      |where
      |        (
      |                p_partkey = l_partkey
      |                and p_brand = 'Brand#32'
      |                and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
      |                and l_quantity >= 7 and l_quantity <= 7 + 10
      |                and p_size between 1 and 5
      |                and l_shipmode in ('AIR', 'AIR REG')
      |                and l_shipinstruct = 'DELIVER IN PERSON'
      |        )
      |        or
      |        (
      |                p_partkey = l_partkey
      |                and p_brand = 'Brand#35'
      |                and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
      |                and l_quantity >= 15 and l_quantity <= 15 + 10
      |                and p_size between 1 and 10
      |                and l_shipmode in ('AIR', 'AIR REG')
      |                and l_shipinstruct = 'DELIVER IN PERSON'
      |        )
      |        or
      |        (
      |                p_partkey = l_partkey
      |                and p_brand = 'Brand#24'
      |                and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
      |                and l_quantity >= 26 and l_quantity <= 26 + 10
      |                and p_size between 1 and 15
      |                and l_shipmode in ('AIR', 'AIR REG')
      |                and l_shipinstruct = 'DELIVER IN PERSON'
      |        )""".stripMargin,*/
/*
    """-- explain formatted
      |with tmp1 as (
      |    select p_partkey from part where p_name like 'forest%'
      |),
      |tmp2 as (
      |    select s_name, s_address, s_suppkey
      |    from supplier, nation
      |    where s_nationkey = n_nationkey
      |    and n_name = 'CANADA'
      |),
      |tmp3 as (
      |    select l_partkey, 0.5 * sum(l_quantity) as sum_quantity, l_suppkey
      |    from lineitem, tmp2
      |    where l_shipdate >= '1994-01-01' and l_shipdate <= '1995-01-01'
      |    and l_suppkey = s_suppkey
      |    group by l_partkey, l_suppkey
      |),
      |tmp4 as (
      |    select ps_partkey, ps_suppkey, ps_availqty
      |    from partsupp
      |    where ps_partkey IN (select p_partkey from tmp1)
      |),
      |tmp5 as (
      |select
      |    ps_suppkey
      |from
      |    tmp4, tmp3
      |where
      |    ps_partkey = l_partkey
      |    and ps_suppkey = l_suppkey
      |    and ps_availqty > sum_quantity
      |)
      |select
      |    s_name,
      |    s_address
      |from
      |    supplier
      |where
      |    s_suppkey IN (select ps_suppkey from tmp5)
      |order by s_name""".stripMargin*/
  )

  // Run all queries for the specified table
  private def time(code: => Unit): Double = {
    val start = System.currentTimeMillis()
    code
    // return time in second
    (System.currentTimeMillis() - start).toDouble / 1000
  }

  def evaluateTimeForQuery(queryList: List[String], spark: SparkSession): List[(String, Double)] = {
    queryList.map { query =>
      println(
        " -----------------------------------------------------------------------------------------")
      println(
        "                                      Executing Query :                                 " +
        "    \n" +
        query)
      println(
        " -----------------------------------------------------------------------------------------")

      val queryTime = time {

        println("count "+ spark.sql(query).count())
      }

      (query.replaceAll("  ", ""), queryTime)
    }
  }

  def writeResults(content: String, file: String) = {
    scala.tools.nsc.io.File(file).appendAll(content)
  }
}