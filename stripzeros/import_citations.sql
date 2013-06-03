.mode csv
.separator ||
CREATE TABLE citation (
  Patent VARCHAR(8),      Cit_Date INTEGER,       Cit_Name VARCHAR(10),
  Cit_Kind VARCHAR(1),    Cit_Country VARCHAR(2), Citation VARCHAR(8),
  Category VARCHAR(15),   CitSeq INTEGER);
.import citation_tmp.csv citation
-- create unique index uqCit on citation (Patent, CitSeq);
create index uqCit on citation (Patent, CitSeq);
