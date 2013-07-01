  select  year(date), count(*)
    from  patent
group by  year(date)