# parse_airflow
1. Task to take min and max rates (also currency, exchange name) from the given data in "https://kurs.kz/". Interesting task with unusual access to the rates
   Details: Currency rates in the <script>. So, it doesn't work to just take rates from soup.find_all('span', class_='svelte-sdi4lo'), also API is not accessible. However, i found a solution. It wasn't so efficient but it works to solve the task on the given requirements.
