SELECT --фінальний селект, який включ 4 стовпці
promo_signup_flag, 
cohort_month, 
month_offset_3,
count(distinct user_id) as users_total --підраховує к-сть унікал юзерів
FROM -- з агрегованої таблиці
(
WITH User_cte as (
    WITH U_Cleared_date as ( -- Крок 1.1 очищуємо та форматуємо дату в табл юзерів--
  	SELECT user_id, promo_signup_flag,
 	replace(replace(TRIM(LEFT(signup_datetime,-5)),'/','.'),'-','.') as normalized_signup_datetime
 	--на 1-му етапі за доп left та trim відсікла час та забрала зайві пробіли. В данному випадку 
 	--ЛЕФТ працює, хоча розумію, що на великиких масивах даних буде складно перевірити, і що краще 
 	--брати функцію спліт_парт (який далі використала з табл event), 2replace замінюють "/" та "-" на крапку
  	FROM public.cohort_users_raw)
		SELECT user_id,promo_signup_flag, normalized_signup_datetime, -- Крок 1.2: Один великий блок, який збирає дату з 1-ої таблиці
    	TO_DATE(
        LPAD(split_part(normalized_signup_datetime, '.', 1), 2, '0') || '-' || -- 1.перевіряємо формат дня і додаємо 0 при потребі + "-" + 
        LPAD(split_part(normalized_signup_datetime, '.', 2), 2, '0') || '-' || -- 2. перевіряємо формат міс і додаємо 0 при потребі + "-" +
        CASE  -- 3. перевіряємо 3 елемент (рік) та після пеевірки його довжини долаємо йог знач
            WHEN length(split_part(normalized_signup_datetime, '.', 3)) = 2 --якщо довжина 2 знач
            THEN '20' || split_part(normalized_signup_datetime, '.', 3)  -- додаємо "20" до 3-го значення розділеної сплітом дати
            ELSE split_part(normalized_signup_datetime, '.', 3) -- інакше, долаємо 3-те значення без змін 
        END, 'DD-MM-YYYY')::timestamp AS signup_date -- Перетворюємо (::) "склеєну" дату на TIMESTAMP та через as присвоюємо ім'я
		FROM U_Cleared_date),
events_cte as (
    WITH E_Cleared_date as ( --Крок 2 - приводимо до єдиного формату табл Event)
	select user_id, event_datetime, event_type,
	replace(replace(SPLIT_PART(trim(event_datetime), ' ', 1),'/','.'),'-','.') as normalized_event_datetime
	--Трім - прибираємо пробіли, спліт-парт - залишаємо 1 частину рядка розділену пробілом, 2replace замінюють "/" та "-" на крапку 
	from public.cohort_events_raw)
		select user_id, event_type,
 		case  
		when normalized_event_datetime ~'^\d{2}\.\d{2}\.\d{4}$'
   		THEN TO_DATE(normalized_event_datetime, 'dd-mm-yyyy')::timestamp
   		--з відповіді ШІ: Використовуйте функцію TO_TIMESTAMP з маскою формату DD.MM.YY. PostgreSQL автоматично розпізнає одинарні цифри місяця
    	ELSE to_timestamp(normalized_event_datetime, 'DD.MM.YY')::timestamp 
 		END AS event_date
		from E_Cleared_date)
SELECT 	-- крок 3 - об'єднання 2-х таблиць за user_id
u.user_id, u.promo_signup_flag, u.signup_date,
e.event_type, e.event_date, 
date_trunc('month', u.signup_date)::date as cohort_month, --додаємо колонку з місяцем когорти та відображ як дата (без часу)
date_trunc('month', e.event_date) as event_month, --додаємо колонку з місяцем події (P.S.залишила час просто для перевірки відображення)
--вар 1. визначаємо зсув, оскільки спостереж в межах 1го року, то вираховуємо зсув по місяцях
(extract('month' from e.event_date) - extract('month' from u.signup_date)) as month_offset_1,
--вар 2 для розрахунку зсуву (теж по місяцях)
(date_part('month', e.event_date)- date_part('month', u.signup_date)) as month_offset_2,
--вар 3 розрахунку зсуву, якбі спостереження ооплювало декілька років
(extract(year from e.event_date) - extract(year from u.signup_date)) * 12 +
    (extract(month from e.event_date) - extract(month from u.signup_date)) as month_offset_3
from User_cte u
join events_cte e
ON u.user_id = e.user_id
WHERE  --додаємо фільтрацію
    u.signup_date is not null           -- Користувачі з датою реєстрації
    and e.event_date is not null        -- Події з датою
    and e.event_type is not null        -- Тип події не null
    and e.event_type != 'test_event'    -- Виключаємо тестові події
) as base_table --назва агрегованої таблиці
WHERE 
    event_month >= '2025-01-01' AND event_month <= '2025-06-30' --період активності
GROUP BY 
    promo_signup_flag, 
    cohort_month, 
    month_offset_3
ORDER BY 
    promo_signup_flag, -- Сортування за групою (promo/non-promo)
    cohort_month,      -- Сортування за місяцем когорти
    month_offset_3;    -- Сортування за номером місяця зсуву