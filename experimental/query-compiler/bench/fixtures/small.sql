select a.name,count(),sum(p.v+a.w+b.w),min(p.v+a.w+b.w),max(p.v+a.w+b.w),avg(p.v+a.w+b.w),sum(p.v/8.0),count(a.name) from p join av a on p.k=a.k join b on a.nk=b.k group by a.name;
