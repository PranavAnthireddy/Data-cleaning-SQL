-- Data Cleaning
select * 
from layoffs;

-- Remove Duplicates
-- Standardize data
-- Null values
-- Remove columns if not necessary

create table layoffs_staging 
like layoffs;

select * from layoffs_staging;

insert layoffs_staging
select * from layoffs;

with duplicates_cte as (
select *, 
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select * 
from duplicates_cte
where row_num > 1;

select * from layoffs_staging
where company = 'Casper';

select company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
from layoffs
group by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
having count(*) > 1;

select *, 
row_number() over(partition by  company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_staging2
select *, 
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;



SET SQL_SAFE_UPDATES = 0;
delete 
from layoffs_staging2
where row_num > 1;
SET SQL_SAFE_UPDATES = 1;

select *
from layoffs_staging2
where row_num > 1;

select * from layoffs_staging2;

-- Standardizing data
select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select *
from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct industry
from layoffs_staging2;

select distinct country
from layoffs_staging2
order by 1;

select country, trim(trailing '.' from country)
from layoffs_staging2;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country = 'United States%';

select distinct country from layoffs_staging2;

select date from layoffs_staging2;

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table layoffs_staging2
modify column `date` date;

select * from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

select *
from layoffs_staging2
where industry is null 
or industry = '';

select * 
from layoffs_staging2
where company = 'Airbnb';

update layoffs_staging2
set industry = 'Travel'
where company = 'Airbnb' and total_laid_off = 30; 

select t1.company, t1.industry, t2.company, t2.industry
from layoffs_staging2 as t1
join layoffs_staging2 as t2
	on t1.company = t2.company;

update layoffs_staging2
set industry = null
where industry = '';

update layoffs_staging2 as t1
join layoffs_staging2 as t2
on t1.company = t2.company
set t1.industry = t2.industry
where (t1.industry is null or t1.industry = '')
and (t2.industry is not null);

select *
from layoffs_staging2
where company = "Bally's Interactive";

-- remove any columns/rows if necessary

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

alter table layoffs_staging2
drop column row_num;

select * from layoffs_staging2;
