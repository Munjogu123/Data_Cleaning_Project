USE world_layoffs;


-- Creating a staging table
CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT INTO layoffs_staging
SELECT *
FROM layoffs;

-- Creating a second staging table to add 'row_num' column
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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
					PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) row_num
FROM layoffs_staging;

-- Removing Duplicates
-- Using a CTE to find duplicate rows utlizing the ROW_NUMBER() window function
WITH duplicate_cte AS (
	SELECT *,
	ROW_NUMBER() OVER(
	PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) row_num
	FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Removing the duplicate rows using DELETE statement
DELETE
FROM layoffs_staging2
WHERE row_num > 1;


-- Standardizing the Data
-- Removing Whitespaces from the values in the company column
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Changing industry with the value 'CryptoCurrency' to 'Crypto'
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Change the data-type of the `date` column
-- Converting the date to datetime format but it is still a string
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Converting `date` from a string to date data-type
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Remove trailing '.' from United States in country column
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';


-- Looking at Null and Blank Values
-- Converting all blank values in industry to NULL
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Updates the NULL values to their corresponding industry names based on the data from the same company
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;


-- Removing unnecessary columns and rows
-- Deleting rows where total_laid_off and percentage_laid_off are NULL
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Deleting the row_num column
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
