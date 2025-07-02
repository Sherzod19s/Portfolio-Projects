SELECT * 
FROM covid_vac.covidvaccinations
Order by 3,4;

SELECT * 
FROM new_schema.coviddeaths
order by 3,4;

SELECT location, date, total_cases, new_cases, total_deaths, population 
FROM new_schema.coviddeaths
order by 1,2;


-- Looking total cases vs total deaths
SELECT location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 AS death_percentage
FROM new_schema.coviddeaths
WHERE location like '%stan%'
order by 1, 2;


-- total cases vs total population 
-- shows what percentage of population got Covid
SELECT location, date, total_cases, population, (total_cases/population)*100 AS cases_percentage
FROM new_schema.coviddeaths
WHERE location like '%states%'
order by 1,2;

-- countries with highest Infection Rate vs Population
SELECT location, population, max(total_cases) as highestinfection_count, max(total_cases/population)*100 AS cases_percentage
FROM new_schema.coviddeaths
group by location, population
order by cases_percentage desc;


-- countries with highest Death Rate vs Population
SELECT location, max(total_cases) as highestinfection_count, max(total_deaths) as Total_deathCount, max(total_deaths)/max(total_cases)*100 AS death_percentage
FROM new_schema.coviddeaths
group by location
order by death_percentage desc;

-- countries with highest Death Rate vs Population
SELECT location, max(cast(total_deaths as unsigned)) as Total_deathCount
FROM new_schema.coviddeaths
WHERE TRIM(continent) <> ''
group by location
order by Total_deathCount desc;

-- continents with highest Death Rates
SELECT location, max(cast(total_deaths as unsigned)) as Total_deathCount, sum(Total_deathCount) as world_Total_deathCount
FROM new_schema.coviddeaths
WHERE TRIM(continent) = ''
group by location
order by Total_deathCount desc;

-- world's total death count
SELECT 
    SUM(Total_deathCount) AS world_Total_deathCount
FROM (
    SELECT location, 
           MAX(CAST(total_deaths AS UNSIGNED)) AS Total_deathCount
    FROM new_schema.coviddeaths
    WHERE TRIM(continent) <> '' AND continent IS NOT NULL
    GROUP BY location
) AS country_deaths;


SELECT location, total_cases, total_deaths
FROM new_schema.coviddeaths
WHERE location = 'Iran'
  AND total_cases = (
    SELECT MAX(total_cases)
    FROM new_schema.coviddeaths
    WHERE location = 'Iran'
  );


-- Global numbers

SELECT 
    date, 
    SUM(new_cases) AS total_cases,
    SUM(CAST(new_deaths AS UNSIGNED)) AS total_deathss,
    SUM(CAST(new_deaths AS UNSIGNED)) / SUM(new_cases) * 100 AS deathPercent
FROM coviddeaths
WHERE TRIM(continent) <> ''
  AND new_deaths REGEXP '^[0-9]+$'
GROUP BY date
ORDER BY date;


SELECT
    SUM(new_cases) AS total_cases,
    SUM(CAST(new_deaths AS UNSIGNED)) AS total_deathss,
    SUM(CAST(new_deaths AS UNSIGNED)) / SUM(new_cases) * 100 AS deathPercent
FROM coviddeaths
WHERE TRIM(continent) <> ''
  AND new_deaths REGEXP '^[0-9]+$';


-- Total population vs vaccinations
SELECT * 
FROM coviddeaths cd
JOIN covid_vac.covidvaccinations cv
	on  cd.location = cv.location
    and cd.date = cv.date;
    
SELECT cd.continent, cd.location, cd.date, cd.population, cv.new_vaccinations
FROM coviddeaths cd
JOIN covid_vac.covidvaccinations cv
	on  cd.location = cv.location
    and cd.date = cv.date
WHERE TRIM(cd.continent) <> '' 
ORDER BY 2,3; 


SELECT cd.continent, cd.location, cd.date, cd.population, cv.new_vaccinations, SUM(convert(cv.new_vaccinations, unsigned)) 
OVER (Partition by cd.location Order BY cd.location, cd.date) as Rolling_people_vac
FROM coviddeaths cd
JOIN covid_vac.covidvaccinations cv
	on  cd.location = cv.location
    and cd.date = cv.date
WHERE TRIM(cd.continent) <> ''
ORDER BY 2,3;
 

-- Using CTE
 WITH PopvsVac(continent, location, date, population, new_vaccinations, Rolling_people_vac) 
 as 
 (
SELECT cd.continent, cd.location, cd.date, cd.population, cv.new_vaccinations, SUM(convert(cv.new_vaccinations, unsigned)) 
OVER (Partition by cd.location Order BY cd.location, cd.date) as Rolling_people_vac
FROM coviddeaths cd
JOIN covid_vac.covidvaccinations cv
	on  cd.location = cv.location
    and cd.date = cv.date
WHERE TRIM(cd.continent) <> '')
-- ORDER BY 2,3;
SELECT *, (Rolling_people_vac/population) * 100 
FROM PopvsVac;


-- Temp Table
Drop table if exists PercentPopulationVaccinated;
Create Table PercentPopulationVaccinated
( 
continent nvarchar(255), 
location nvarchar(255), 
date datetime, 
population numeric, 
new_vaccinations numeric, 
Rolling_people_vac numeric
);
INSERT INTO PercentPopulationVaccinated
SELECT 
    cd.continent, 
    cd.location, 
    cd.date, 
    cd.population, 
    cv.new_vaccinations,
    SUM(cv.new_vaccinations) OVER (PARTITION BY cd.location ORDER BY cd.date) AS Rolling_people_vac
FROM coviddeaths cd
JOIN covid_vac.covidvaccinations cv
    ON cd.location = cv.location
   AND cd.date = cv.date
WHERE TRIM(cd.continent) <> ''
AND cv.new_vaccinations REGEXP '^[0-9]+$';

-- ORDER BY 2,3; 
SELECT *, 
       (Rolling_people_vac / population) * 100 AS percent_vaccinated
FROM PercentPopulationVaccinated;


-- Creating view

Create view PercentPopulationVaccinated1 as 
SELECT cd.continent, cd.location, cd.date, cd.population, cv.new_vaccinations, SUM(convert(cv.new_vaccinations, unsigned)) 
OVER (Partition by cd.location Order BY cd.location, cd.date) as Rolling_people_vac
FROM coviddeaths cd
JOIN covid_vac.covidvaccinations cv
	on  cd.location = cv.location
    and cd.date = cv.date
WHERE TRIM(cd.continent) <> '';
-- ORDER BY 2,3;
 
SELECT * 
from PercentPopulationVaccinated1