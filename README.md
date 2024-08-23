# Parse Currency Rates
1) Python
2) Apache Airflow
3) Docker

Objective: To extract the minimum and maximum currency rates (along with the currency type and exchange name) from the website "kurs.kz". This task presented an interesting challenge due to the unconventional method required to access the currency rates.

Details: The currency rates are embedded within a <script> tag, making it impossible to simply extract the data using soup.find_all('span', class_='svelte-sdi4lo'). Additionally, there is no accessible API. However, I found a solution. Although it involved manually specifying the required currencies, which isn't the most efficient method, it effectively meets the task requirements.


