import duckdb

con = duckdb.connect()
con.execute("INSTALL sqlite; LOAD sqlite; ATTACH 'owl.db' AS owl (TYPE SQLITE);")

print("Sectors Table")
print(con.execute("select * from owl.sectors order by 1,2,3 limit 10;").pl())

print("Companies Table")
print(con.execute("select * from owl.companies order by 1,2,3 limit 10;").pl())

print("Prices Table, 10 rows")
print(con.execute("select * from owl.prices order by 1,2,3 limit 10;").pl())

print("load_runs Table")
print(con.execute("select * from owl.load_runs order by 1,2,3 limit 10;").pl())

print("Cumulative return per company:")
print(con.execute("""
    SELECT c.name,
            first(p.close_usd ORDER BY p.asof) AS first_close,
            last(p.close_usd  ORDER BY p.asof) AS last_close,
            last(p.close_usd  ORDER BY p.asof) / first(p.close_usd ORDER BY p.asof) - 1 AS cumulative_return
    FROM owl.prices p 
    JOIN owl.companies c 
    on p.company_id = c.company_id
    GROUP BY c.name
    ORDER BY cumulative_return DESC
    ;
    """).pl())


print("Cumulative return per company and Sector:")
print(con.execute("""
    SELECT c.name,
            s.sector_level1,
            s.sector_level2,
            first(p.close_usd ORDER BY p.asof) AS first_close,
            last(p.close_usd  ORDER BY p.asof) AS last_close,
            last(p.close_usd  ORDER BY p.asof) / first(p.close_usd ORDER BY p.asof) - 1 AS cumulative_return
    FROM owl.prices p 
    JOIN owl.companies c 
    on p.company_id = c.company_id
    JOIN owl.sectors s
    on s.sector_id = c.sector_id
    GROUP BY c.name, s.sector_level1, s.sector_level2
    ORDER BY cumulative_return DESC
    ;
    """).pl())
