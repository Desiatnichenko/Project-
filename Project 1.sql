INSERT INTO "WITH fb AS ( 
    SELECT
        ad_date,
        url_parameters,
        COALESCE(spend, 0) AS spend,
        COALESCE(impressions, 0) AS impressions,
        COALESCE(reach, 0) AS reach,
        COALESCE(clicks, 0) AS clicks,
        COALESCE(leads, 0) AS leads,
        COALESCE(value, 0) AS value
    FROM
        facebook_ads_basic_daily fabd
    LEFT JOIN facebook_campaign fc ON fabd.campaign_id = fc.campaign_id
    LEFT JOIN facebook_adset fa ON fabd.adset_id = fa.adset_id 
    UNION ALL
    SELECT
        ad_date,
        url_parameters,
        COALESCE(spend, 0) AS spend,
        COALESCE(impressions, 0) AS impressions,
        COALESCE(reach, 0) AS reach,
        COALESCE(clicks, 0) AS clicks,
        COALESCE(leads, 0) AS leads,
        COALESCE(value, 0) AS value
    FROM
        google_ads_basic_daily gabd
    WHERE 
        campaign_name IS NOT NULL
),
gather AS (
    SELECT * FROM fb
),
aggregated_data AS (
    SELECT
        DATE_TRUNC('month', ad_date)::date AS ad_month,
        CASE
            WHEN LOWER(SUBSTRING(url_parameters FROM 'utm_campaign=([^&]+)')) != 'nan' THEN LOWER(SUBSTRING(url_parameters FROM 'utm_campaign=([^&]+)'))
            ELSE null
        END AS utm_campaign,
        ROUND(SUM(spend)::NUMERIC, 3) AS total_spend,
        SUM(impressions) AS total_impressions,
        SUM(clicks) AS total_clicks,
        SUM(value) AS total_value,
        CASE
            WHEN SUM(impressions) = 0 THEN 0
            ELSE ROUND((SUM(clicks)::NUMERIC / SUM(impressions)), 3)
        END AS CTR,
        CASE
            WHEN SUM(clicks) = 0 THEN 0
            ELSE ROUND((SUM(spend)::NUMERIC / SUM(clicks)), 3)
        END AS CPC,
        CASE
            WHEN SUM(impressions) = 0 THEN 0
            ELSE ROUND((SUM(spend)::NUMERIC * 1000 / SUM(impressions)), 3)
        END AS CPM,
        CASE
            WHEN SUM(spend) = 0 THEN 0
            ELSE ROUND(((SUM(value)::NUMERIC - SUM(spend)) / SUM(spend) * 100), 3)
        END AS ROMI
    FROM gather
    GROUP BY ad_month, utm_campaign
),
with_previous AS (
    SELECT
        ad_month,
        utm_campaign,
        total_spend,
        total_impressions,
        total_clicks,
        total_value,
        CTR,
        CPC,
        CPM,
        ROMI,
        LAG(CPM) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_CPM,
        LAG(CTR) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_CTR,
        LAG(ROMI) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_ROMI
    FROM aggregated_data
)
SELECT
    ad_month,
    utm_campaign,
    total_spend,
    total_impressions,
    total_clicks,
    total_value,
    CTR,
    CPC,
    CPM,
    ROMI,
    ROUND(
        (CPM - prev_CPM) / NULLIF(prev_CPM, 0) * 100,
        2
    ) AS CPM_difference,
    ROUND(
        (CTR - prev_CTR) / NULLIF(prev_CTR, 0) * 100,
        2
    ) AS CTR_difference,
    ROUND(
        (ROMI - prev_ROMI) / NULLIF(prev_ROMI, 0) * 100,
        2
    ) AS ROMI_difference
FROM with_previous
ORDER BY utm_campaign, ad_month" (ad_month,utm_campaign,total_spend,total_impressions,total_clicks,total_value,ctr,cpc,cpm,romi,cpm_difference,ctr_difference,romi_difference) VALUES
	 ('2021-06-01','cheap',14052.000,15672,166,17941,0.011,84.651,896.631,27.676,NULL,NULL,NULL),
	 ('2021-07-01','cheap',256706.000,471800,2648,305536,0.006,96.943,544.099,19.022,-39.32,-45.45,-31.27),
	 ('2021-08-01','cheap',94110.000,201544,1118,105092,0.006,84.177,466.945,11.669,-14.18,0.00,-38.66),
	 ('2021-09-01','cheap',242251.000,475567,2624,295481,0.006,92.321,509.394,21.973,9.09,0.00,88.30),
	 ('2021-10-01','cheap',226921.000,368062,2328,269827,0.006,97.475,616.529,18.908,21.03,0.00,-13.95),
	 ('2021-11-01','cheap',178323.000,326649,2190,217624,0.007,81.426,545.916,22.039,-11.45,16.67,16.56),
	 ('2021-12-01','cheap',154076.000,341702,1827,180571,0.005,84.333,450.908,17.196,-17.40,-28.57,-21.97),
	 ('2022-01-01','cheap',0.000,0,0,0,0,0,0,0,-100.00,-100.00,-100.00),
	 ('2021-01-01','%d0%b1%d1%80%d0%b5%d0%bd%d0%b4',806.000,10234,13,854,0.001,62.000,78.757,5.955,NULL,NULL,NULL),
	 ('2021-02-01','%d0%b1%d1%80%d0%b5%d0%bd%d0%b4',596.000,40669,18,663,0.000,33.111,14.655,11.242,-81.39,-100.00,88.78);
INSERT INTO "WITH fb AS ( 
    SELECT
        ad_date,
        url_parameters,
        COALESCE(spend, 0) AS spend,
        COALESCE(impressions, 0) AS impressions,
        COALESCE(reach, 0) AS reach,
        COALESCE(clicks, 0) AS clicks,
        COALESCE(leads, 0) AS leads,
        COALESCE(value, 0) AS value
    FROM
        facebook_ads_basic_daily fabd
    LEFT JOIN facebook_campaign fc ON fabd.campaign_id = fc.campaign_id
    LEFT JOIN facebook_adset fa ON fabd.adset_id = fa.adset_id 
    UNION ALL
    SELECT
        ad_date,
        url_parameters,
        COALESCE(spend, 0) AS spend,
        COALESCE(impressions, 0) AS impressions,
        COALESCE(reach, 0) AS reach,
        COALESCE(clicks, 0) AS clicks,
        COALESCE(leads, 0) AS leads,
        COALESCE(value, 0) AS value
    FROM
        google_ads_basic_daily gabd
    WHERE 
        campaign_name IS NOT NULL
),
gather AS (
    SELECT * FROM fb
),
aggregated_data AS (
    SELECT
        DATE_TRUNC('month', ad_date)::date AS ad_month,
        CASE
            WHEN LOWER(SUBSTRING(url_parameters FROM 'utm_campaign=([^&]+)')) != 'nan' THEN LOWER(SUBSTRING(url_parameters FROM 'utm_campaign=([^&]+)'))
            ELSE null
        END AS utm_campaign,
        ROUND(SUM(spend)::NUMERIC, 3) AS total_spend,
        SUM(impressions) AS total_impressions,
        SUM(clicks) AS total_clicks,
        SUM(value) AS total_value,
        CASE
            WHEN SUM(impressions) = 0 THEN 0
            ELSE ROUND((SUM(clicks)::NUMERIC / SUM(impressions)), 3)
        END AS CTR,
        CASE
            WHEN SUM(clicks) = 0 THEN 0
            ELSE ROUND((SUM(spend)::NUMERIC / SUM(clicks)), 3)
        END AS CPC,
        CASE
            WHEN SUM(impressions) = 0 THEN 0
            ELSE ROUND((SUM(spend)::NUMERIC * 1000 / SUM(impressions)), 3)
        END AS CPM,
        CASE
            WHEN SUM(spend) = 0 THEN 0
            ELSE ROUND(((SUM(value)::NUMERIC - SUM(spend)) / SUM(spend) * 100), 3)
        END AS ROMI
    FROM gather
    GROUP BY ad_month, utm_campaign
),
with_previous AS (
    SELECT
        ad_month,
        utm_campaign,
        total_spend,
        total_impressions,
        total_clicks,
        total_value,
        CTR,
        CPC,
        CPM,
        ROMI,
        LAG(CPM) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_CPM,
        LAG(CTR) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_CTR,
        LAG(ROMI) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_ROMI
    FROM aggregated_data
)
SELECT
    ad_month,
    utm_campaign,
    total_spend,
    total_impressions,
    total_clicks,
    total_value,
    CTR,
    CPC,
    CPM,
    ROMI,
    ROUND(
        (CPM - prev_CPM) / NULLIF(prev_CPM, 0) * 100,
        2
    ) AS CPM_difference,
    ROUND(
        (CTR - prev_CTR) / NULLIF(prev_CTR, 0) * 100,
        2
    ) AS CTR_difference,
    ROUND(
        (ROMI - prev_ROMI) / NULLIF(prev_ROMI, 0) * 100,
        2
    ) AS ROMI_difference
FROM with_previous
ORDER BY utm_campaign, ad_month" (ad_month,utm_campaign,total_spend,total_impressions,total_clicks,total_value,ctr,cpc,cpm,romi,cpm_difference,ctr_difference,romi_difference) VALUES
	 ('2021-10-01','%d0%b1%d1%80%d0%b5%d0%bd%d0%b4',227.000,351,0,333,0.000,0,646.724,46.696,4312.99,NULL,315.37),
	 ('2021-09-01','%d1%82%d1%80%d0%b5%d0%bd%d0%b4',449346.000,1001096,7645,562294,0.008,58.776,448.854,25.136,NULL,NULL,NULL),
	 ('2021-10-01','%d1%82%d1%80%d0%b5%d0%bd%d0%b4',443003.000,1118697,13778,561061,0.012,32.153,395.999,26.649,-11.78,50.00,6.02),
	 ('2021-11-01','%d1%82%d1%80%d0%b5%d0%bd%d0%b4',412761.000,1462559,14663,506407,0.010,28.150,282.218,22.688,-28.73,-16.67,-14.86),
	 ('2021-12-01','%d1%82%d1%80%d0%b5%d0%bd%d0%b4',648202.000,2004616,16117,839665,0.008,40.219,323.355,29.538,14.58,-20.00,30.19),
	 ('2022-01-01','%d1%82%d1%80%d0%b5%d0%bd%d0%b4',44703.000,464338,3210,86524,0.007,13.926,96.273,93.553,-70.23,-12.50,216.72),
	 ('2022-02-01','%d1%82%d1%80%d0%b5%d0%bd%d0%b4',31060.000,353799,2166,60761,0.006,14.340,87.790,95.625,-8.81,-14.29,2.21),
	 ('2020-11-01','discounts',57355.000,285658,9270,69795,0.032,6.187,200.782,21.689,NULL,NULL,NULL),
	 ('2020-12-01','discounts',114337.000,364293,10305,141798,0.028,11.095,313.860,24.018,56.32,-12.50,10.74),
	 ('2021-01-01','discounts',174720.000,983544,24067,213785,0.024,7.260,177.643,22.359,-43.40,-14.29,-6.91);
INSERT INTO "WITH fb AS ( 
    SELECT
        ad_date,
        url_parameters,
        COALESCE(spend, 0) AS spend,
        COALESCE(impressions, 0) AS impressions,
        COALESCE(reach, 0) AS reach,
        COALESCE(clicks, 0) AS clicks,
        COALESCE(leads, 0) AS leads,
        COALESCE(value, 0) AS value
    FROM
        facebook_ads_basic_daily fabd
    LEFT JOIN facebook_campaign fc ON fabd.campaign_id = fc.campaign_id
    LEFT JOIN facebook_adset fa ON fabd.adset_id = fa.adset_id 
    UNION ALL
    SELECT
        ad_date,
        url_parameters,
        COALESCE(spend, 0) AS spend,
        COALESCE(impressions, 0) AS impressions,
        COALESCE(reach, 0) AS reach,
        COALESCE(clicks, 0) AS clicks,
        COALESCE(leads, 0) AS leads,
        COALESCE(value, 0) AS value
    FROM
        google_ads_basic_daily gabd
    WHERE 
        campaign_name IS NOT NULL
),
gather AS (
    SELECT * FROM fb
),
aggregated_data AS (
    SELECT
        DATE_TRUNC('month', ad_date)::date AS ad_month,
        CASE
            WHEN LOWER(SUBSTRING(url_parameters FROM 'utm_campaign=([^&]+)')) != 'nan' THEN LOWER(SUBSTRING(url_parameters FROM 'utm_campaign=([^&]+)'))
            ELSE null
        END AS utm_campaign,
        ROUND(SUM(spend)::NUMERIC, 3) AS total_spend,
        SUM(impressions) AS total_impressions,
        SUM(clicks) AS total_clicks,
        SUM(value) AS total_value,
        CASE
            WHEN SUM(impressions) = 0 THEN 0
            ELSE ROUND((SUM(clicks)::NUMERIC / SUM(impressions)), 3)
        END AS CTR,
        CASE
            WHEN SUM(clicks) = 0 THEN 0
            ELSE ROUND((SUM(spend)::NUMERIC / SUM(clicks)), 3)
        END AS CPC,
        CASE
            WHEN SUM(impressions) = 0 THEN 0
            ELSE ROUND((SUM(spend)::NUMERIC * 1000 / SUM(impressions)), 3)
        END AS CPM,
        CASE
            WHEN SUM(spend) = 0 THEN 0
            ELSE ROUND(((SUM(value)::NUMERIC - SUM(spend)) / SUM(spend) * 100), 3)
        END AS ROMI
    FROM gather
    GROUP BY ad_month, utm_campaign
),
with_previous AS (
    SELECT
        ad_month,
        utm_campaign,
        total_spend,
        total_impressions,
        total_clicks,
        total_value,
        CTR,
        CPC,
        CPM,
        ROMI,
        LAG(CPM) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_CPM,
        LAG(CTR) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_CTR,
        LAG(ROMI) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_ROMI
    FROM aggregated_data
)
SELECT
    ad_month,
    utm_campaign,
    total_spend,
    total_impressions,
    total_clicks,
    total_value,
    CTR,
    CPC,
    CPM,
    ROMI,
    ROUND(
        (CPM - prev_CPM) / NULLIF(prev_CPM, 0) * 100,
        2
    ) AS CPM_difference,
    ROUND(
        (CTR - prev_CTR) / NULLIF(prev_CTR, 0) * 100,
        2
    ) AS CTR_difference,
    ROUND(
        (ROMI - prev_ROMI) / NULLIF(prev_ROMI, 0) * 100,
        2
    ) AS ROMI_difference
FROM with_previous
ORDER BY utm_campaign, ad_month" (ad_month,utm_campaign,total_spend,total_impressions,total_clicks,total_value,ctr,cpc,cpm,romi,cpm_difference,ctr_difference,romi_difference) VALUES
	 ('2021-02-01','discounts',92267.000,288393,5770,114921,0.020,15.991,319.935,24.553,80.10,-16.67,9.81),
	 ('2021-04-01','discounts',231729.000,982437,8521,269750,0.009,27.195,235.872,16.408,-26.28,-55.00,-33.17),
	 ('2021-05-01','discounts',349642.000,1500379,9502,443275,0.006,36.797,233.036,26.780,-1.20,-33.33,63.21),
	 ('2021-06-01','discounts',275395.000,1074263,9366,378690,0.009,29.404,256.357,37.508,10.01,50.00,40.06),
	 ('2021-07-01','discounts',983729.000,3213086,23894,1182326,0.007,41.171,306.163,20.188,19.43,-22.22,-46.18),
	 ('2021-08-01','discounts',1371088.000,4442424,34524,1751753,0.008,39.714,308.635,27.764,0.81,14.29,37.53),
	 ('2021-09-01','discounts',1337457.000,3580801,22729,1586818,0.006,58.844,373.508,18.644,21.02,-25.00,-32.85),
	 ('2021-10-01','discounts',475123.000,1245258,6252,565704,0.005,75.995,381.546,19.065,2.15,-16.67,2.26),
	 ('2021-06-01','hobby',55354.000,92094,861,67020,0.009,64.290,601.060,21.075,NULL,NULL,NULL),
	 ('2021-07-01','hobby',318827.000,611285,5222,374569,0.009,61.055,521.568,17.483,-13.23,0.00,-17.04);
INSERT INTO "WITH fb AS ( 
    SELECT
        ad_date,
        url_parameters,
        COALESCE(spend, 0) AS spend,
        COALESCE(impressions, 0) AS impressions,
        COALESCE(reach, 0) AS reach,
        COALESCE(clicks, 0) AS clicks,
        COALESCE(leads, 0) AS leads,
        COALESCE(value, 0) AS value
    FROM
        facebook_ads_basic_daily fabd
    LEFT JOIN facebook_campaign fc ON fabd.campaign_id = fc.campaign_id
    LEFT JOIN facebook_adset fa ON fabd.adset_id = fa.adset_id 
    UNION ALL
    SELECT
        ad_date,
        url_parameters,
        COALESCE(spend, 0) AS spend,
        COALESCE(impressions, 0) AS impressions,
        COALESCE(reach, 0) AS reach,
        COALESCE(clicks, 0) AS clicks,
        COALESCE(leads, 0) AS leads,
        COALESCE(value, 0) AS value
    FROM
        google_ads_basic_daily gabd
    WHERE 
        campaign_name IS NOT NULL
),
gather AS (
    SELECT * FROM fb
),
aggregated_data AS (
    SELECT
        DATE_TRUNC('month', ad_date)::date AS ad_month,
        CASE
            WHEN LOWER(SUBSTRING(url_parameters FROM 'utm_campaign=([^&]+)')) != 'nan' THEN LOWER(SUBSTRING(url_parameters FROM 'utm_campaign=([^&]+)'))
            ELSE null
        END AS utm_campaign,
        ROUND(SUM(spend)::NUMERIC, 3) AS total_spend,
        SUM(impressions) AS total_impressions,
        SUM(clicks) AS total_clicks,
        SUM(value) AS total_value,
        CASE
            WHEN SUM(impressions) = 0 THEN 0
            ELSE ROUND((SUM(clicks)::NUMERIC / SUM(impressions)), 3)
        END AS CTR,
        CASE
            WHEN SUM(clicks) = 0 THEN 0
            ELSE ROUND((SUM(spend)::NUMERIC / SUM(clicks)), 3)
        END AS CPC,
        CASE
            WHEN SUM(impressions) = 0 THEN 0
            ELSE ROUND((SUM(spend)::NUMERIC * 1000 / SUM(impressions)), 3)
        END AS CPM,
        CASE
            WHEN SUM(spend) = 0 THEN 0
            ELSE ROUND(((SUM(value)::NUMERIC - SUM(spend)) / SUM(spend) * 100), 3)
        END AS ROMI
    FROM gather
    GROUP BY ad_month, utm_campaign
),
with_previous AS (
    SELECT
        ad_month,
        utm_campaign,
        total_spend,
        total_impressions,
        total_clicks,
        total_value,
        CTR,
        CPC,
        CPM,
        ROMI,
        LAG(CPM) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_CPM,
        LAG(CTR) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_CTR,
        LAG(ROMI) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_ROMI
    FROM aggregated_data
)
SELECT
    ad_month,
    utm_campaign,
    total_spend,
    total_impressions,
    total_clicks,
    total_value,
    CTR,
    CPC,
    CPM,
    ROMI,
    ROUND(
        (CPM - prev_CPM) / NULLIF(prev_CPM, 0) * 100,
        2
    ) AS CPM_difference,
    ROUND(
        (CTR - prev_CTR) / NULLIF(prev_CTR, 0) * 100,
        2
    ) AS CTR_difference,
    ROUND(
        (ROMI - prev_ROMI) / NULLIF(prev_ROMI, 0) * 100,
        2
    ) AS ROMI_difference
FROM with_previous
ORDER BY utm_campaign, ad_month" (ad_month,utm_campaign,total_spend,total_impressions,total_clicks,total_value,ctr,cpc,cpm,romi,cpm_difference,ctr_difference,romi_difference) VALUES
	 ('2021-08-01','hobby',391107.000,995969,6709,459451,0.007,58.296,392.690,17.475,-24.71,-22.22,-0.05),
	 ('2021-09-01','hobby',144079.000,288142,2009,182695,0.007,71.717,500.028,26.802,27.33,0.00,53.37),
	 ('2021-10-01','hobby',104583.000,206089,1114,126338,0.005,93.881,507.465,20.802,1.49,-28.57,-22.39),
	 ('2021-11-01','hobby',61084.000,117795,752,69165,0.006,81.229,518.562,13.229,2.19,20.00,-36.41),
	 ('2021-12-01','hobby',60439.000,166052,597,76205,0.004,101.238,363.976,26.086,-29.81,-33.33,97.19),
	 ('2022-01-01','hobby',0.000,0,0,0,0,0,0,0,-100.00,-100.00,-100.00),
	 ('2021-06-01','presents',8351.000,29799,431,11318,0.014,19.376,280.244,35.529,NULL,NULL,NULL),
	 ('2021-07-01','presents',25198.000,97220,1324,33867,0.014,19.032,259.185,34.404,-7.51,0.00,-3.17),
	 ('2020-12-01','recognition',16618.000,130849,91,20129,0.001,182.615,127.001,21.128,NULL,NULL,NULL),
	 ('2021-01-01','recognition',4594.000,40886,17,5856,0.000,270.235,112.361,27.471,-11.53,-100.00,30.02);
INSERT INTO "WITH fb AS ( 
    SELECT
        ad_date,
        url_parameters,
        COALESCE(spend, 0) AS spend,
        COALESCE(impressions, 0) AS impressions,
        COALESCE(reach, 0) AS reach,
        COALESCE(clicks, 0) AS clicks,
        COALESCE(leads, 0) AS leads,
        COALESCE(value, 0) AS value
    FROM
        facebook_ads_basic_daily fabd
    LEFT JOIN facebook_campaign fc ON fabd.campaign_id = fc.campaign_id
    LEFT JOIN facebook_adset fa ON fabd.adset_id = fa.adset_id 
    UNION ALL
    SELECT
        ad_date,
        url_parameters,
        COALESCE(spend, 0) AS spend,
        COALESCE(impressions, 0) AS impressions,
        COALESCE(reach, 0) AS reach,
        COALESCE(clicks, 0) AS clicks,
        COALESCE(leads, 0) AS leads,
        COALESCE(value, 0) AS value
    FROM
        google_ads_basic_daily gabd
    WHERE 
        campaign_name IS NOT NULL
),
gather AS (
    SELECT * FROM fb
),
aggregated_data AS (
    SELECT
        DATE_TRUNC('month', ad_date)::date AS ad_month,
        CASE
            WHEN LOWER(SUBSTRING(url_parameters FROM 'utm_campaign=([^&]+)')) != 'nan' THEN LOWER(SUBSTRING(url_parameters FROM 'utm_campaign=([^&]+)'))
            ELSE null
        END AS utm_campaign,
        ROUND(SUM(spend)::NUMERIC, 3) AS total_spend,
        SUM(impressions) AS total_impressions,
        SUM(clicks) AS total_clicks,
        SUM(value) AS total_value,
        CASE
            WHEN SUM(impressions) = 0 THEN 0
            ELSE ROUND((SUM(clicks)::NUMERIC / SUM(impressions)), 3)
        END AS CTR,
        CASE
            WHEN SUM(clicks) = 0 THEN 0
            ELSE ROUND((SUM(spend)::NUMERIC / SUM(clicks)), 3)
        END AS CPC,
        CASE
            WHEN SUM(impressions) = 0 THEN 0
            ELSE ROUND((SUM(spend)::NUMERIC * 1000 / SUM(impressions)), 3)
        END AS CPM,
        CASE
            WHEN SUM(spend) = 0 THEN 0
            ELSE ROUND(((SUM(value)::NUMERIC - SUM(spend)) / SUM(spend) * 100), 3)
        END AS ROMI
    FROM gather
    GROUP BY ad_month, utm_campaign
),
with_previous AS (
    SELECT
        ad_month,
        utm_campaign,
        total_spend,
        total_impressions,
        total_clicks,
        total_value,
        CTR,
        CPC,
        CPM,
        ROMI,
        LAG(CPM) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_CPM,
        LAG(CTR) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_CTR,
        LAG(ROMI) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_ROMI
    FROM aggregated_data
)
SELECT
    ad_month,
    utm_campaign,
    total_spend,
    total_impressions,
    total_clicks,
    total_value,
    CTR,
    CPC,
    CPM,
    ROMI,
    ROUND(
        (CPM - prev_CPM) / NULLIF(prev_CPM, 0) * 100,
        2
    ) AS CPM_difference,
    ROUND(
        (CTR - prev_CTR) / NULLIF(prev_CTR, 0) * 100,
        2
    ) AS CTR_difference,
    ROUND(
        (ROMI - prev_ROMI) / NULLIF(prev_ROMI, 0) * 100,
        2
    ) AS ROMI_difference
FROM with_previous
ORDER BY utm_campaign, ad_month" (ad_month,utm_campaign,total_spend,total_impressions,total_clicks,total_value,ctr,cpc,cpm,romi,cpm_difference,ctr_difference,romi_difference) VALUES
	 ('2021-03-01','recognition',5468.000,103559,52,6677,0.001,105.154,52.801,22.110,-53.01,NULL,-19.52),
	 ('2021-04-01','recognition',1855.000,15737,33,2327,0.002,56.212,117.875,25.445,123.24,100.00,15.08),
	 ('2021-06-01','recognition',7982.000,164738,64,11893,0.000,124.719,48.453,48.998,-58.89,-100.00,92.56),
	 ('2021-07-01','recognition',1119.000,22382,17,1465,0.001,65.824,49.996,30.920,3.18,NULL,-36.90),
	 ('2021-08-01','recognition',8090.000,141043,55,9239,0.000,147.091,57.358,14.203,14.73,-100.00,-54.07),
	 ('2021-09-01','recognition',5319.000,105324,124,6929,0.001,42.895,50.501,30.269,-11.95,NULL,113.12),
	 ('2021-11-01','recognition',7982.000,153559,79,7927,0.001,101.038,51.980,-0.689,2.93,0.00,-102.28),
	 ('2021-06-01','startup',15701.000,50126,586,22226,0.012,26.794,313.231,41.558,NULL,NULL,NULL),
	 ('2021-07-01','startup',7.000,52,0,7,0.000,0,134.615,0.000,-57.02,-100.00,-100.00),
	 ('2022-03-01','top_gear',21397.000,8610,156,21336,0.018,137.160,2485.134,-0.285,NULL,NULL,NULL);
INSERT INTO "WITH fb AS ( 
    SELECT
        ad_date,
        url_parameters,
        COALESCE(spend, 0) AS spend,
        COALESCE(impressions, 0) AS impressions,
        COALESCE(reach, 0) AS reach,
        COALESCE(clicks, 0) AS clicks,
        COALESCE(leads, 0) AS leads,
        COALESCE(value, 0) AS value
    FROM
        facebook_ads_basic_daily fabd
    LEFT JOIN facebook_campaign fc ON fabd.campaign_id = fc.campaign_id
    LEFT JOIN facebook_adset fa ON fabd.adset_id = fa.adset_id 
    UNION ALL
    SELECT
        ad_date,
        url_parameters,
        COALESCE(spend, 0) AS spend,
        COALESCE(impressions, 0) AS impressions,
        COALESCE(reach, 0) AS reach,
        COALESCE(clicks, 0) AS clicks,
        COALESCE(leads, 0) AS leads,
        COALESCE(value, 0) AS value
    FROM
        google_ads_basic_daily gabd
    WHERE 
        campaign_name IS NOT NULL
),
gather AS (
    SELECT * FROM fb
),
aggregated_data AS (
    SELECT
        DATE_TRUNC('month', ad_date)::date AS ad_month,
        CASE
            WHEN LOWER(SUBSTRING(url_parameters FROM 'utm_campaign=([^&]+)')) != 'nan' THEN LOWER(SUBSTRING(url_parameters FROM 'utm_campaign=([^&]+)'))
            ELSE null
        END AS utm_campaign,
        ROUND(SUM(spend)::NUMERIC, 3) AS total_spend,
        SUM(impressions) AS total_impressions,
        SUM(clicks) AS total_clicks,
        SUM(value) AS total_value,
        CASE
            WHEN SUM(impressions) = 0 THEN 0
            ELSE ROUND((SUM(clicks)::NUMERIC / SUM(impressions)), 3)
        END AS CTR,
        CASE
            WHEN SUM(clicks) = 0 THEN 0
            ELSE ROUND((SUM(spend)::NUMERIC / SUM(clicks)), 3)
        END AS CPC,
        CASE
            WHEN SUM(impressions) = 0 THEN 0
            ELSE ROUND((SUM(spend)::NUMERIC * 1000 / SUM(impressions)), 3)
        END AS CPM,
        CASE
            WHEN SUM(spend) = 0 THEN 0
            ELSE ROUND(((SUM(value)::NUMERIC - SUM(spend)) / SUM(spend) * 100), 3)
        END AS ROMI
    FROM gather
    GROUP BY ad_month, utm_campaign
),
with_previous AS (
    SELECT
        ad_month,
        utm_campaign,
        total_spend,
        total_impressions,
        total_clicks,
        total_value,
        CTR,
        CPC,
        CPM,
        ROMI,
        LAG(CPM) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_CPM,
        LAG(CTR) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_CTR,
        LAG(ROMI) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_ROMI
    FROM aggregated_data
)
SELECT
    ad_month,
    utm_campaign,
    total_spend,
    total_impressions,
    total_clicks,
    total_value,
    CTR,
    CPC,
    CPM,
    ROMI,
    ROUND(
        (CPM - prev_CPM) / NULLIF(prev_CPM, 0) * 100,
        2
    ) AS CPM_difference,
    ROUND(
        (CTR - prev_CTR) / NULLIF(prev_CTR, 0) * 100,
        2
    ) AS CTR_difference,
    ROUND(
        (ROMI - prev_ROMI) / NULLIF(prev_ROMI, 0) * 100,
        2
    ) AS ROMI_difference
FROM with_previous
ORDER BY utm_campaign, ad_month" (ad_month,utm_campaign,total_spend,total_impressions,total_clicks,total_value,ctr,cpc,cpm,romi,cpm_difference,ctr_difference,romi_difference) VALUES
	 ('2022-04-01','top_gear',38020.000,13755,241,40711,0.018,157.759,2764.086,7.078,11.22,0.00,-2583.51),
	 ('2022-03-01','twitch',1586889.000,1027921,15801,1896928,0.015,100.430,1543.785,19.538,NULL,NULL,NULL),
	 ('2022-04-01','twitch',3909094.000,2040830,31451,4679574,0.015,124.292,1915.443,19.710,24.07,0.00,0.88),
	 ('2022-05-01','twitch',173192.000,46530,1091,224887,0.023,158.746,3722.158,29.848,94.32,53.33,51.44),
	 ('2022-03-01','upgrade',260226.000,176595,1120,316641,0.006,232.345,1473.575,21.679,NULL,NULL,NULL),
	 ('2022-04-01','upgrade',1196624.000,792105,4247,1448904,0.005,281.757,1510.689,21.083,2.52,-16.67,-2.75),
	 ('2022-05-01','upgrade',1458505.000,1181820,4641,1782360,0.004,314.265,1234.118,22.205,-18.31,-20.00,5.32),
	 ('2022-06-01','upgrade',1659533.000,848500,3786,1981536,0.004,438.334,1955.843,19.403,58.48,0.00,-12.62),
	 ('2022-07-01','upgrade',677234.000,231760,1272,720557,0.005,532.417,2922.135,6.397,49.41,25.00,-67.03),
	 ('2022-10-01','upgrade',380918.000,189092,702,409839,0.004,542.618,2014.459,7.592,-31.06,-20.00,18.68);
INSERT INTO "WITH fb AS ( 
    SELECT
        ad_date,
        url_parameters,
        COALESCE(spend, 0) AS spend,
        COALESCE(impressions, 0) AS impressions,
        COALESCE(reach, 0) AS reach,
        COALESCE(clicks, 0) AS clicks,
        COALESCE(leads, 0) AS leads,
        COALESCE(value, 0) AS value
    FROM
        facebook_ads_basic_daily fabd
    LEFT JOIN facebook_campaign fc ON fabd.campaign_id = fc.campaign_id
    LEFT JOIN facebook_adset fa ON fabd.adset_id = fa.adset_id 
    UNION ALL
    SELECT
        ad_date,
        url_parameters,
        COALESCE(spend, 0) AS spend,
        COALESCE(impressions, 0) AS impressions,
        COALESCE(reach, 0) AS reach,
        COALESCE(clicks, 0) AS clicks,
        COALESCE(leads, 0) AS leads,
        COALESCE(value, 0) AS value
    FROM
        google_ads_basic_daily gabd
    WHERE 
        campaign_name IS NOT NULL
),
gather AS (
    SELECT * FROM fb
),
aggregated_data AS (
    SELECT
        DATE_TRUNC('month', ad_date)::date AS ad_month,
        CASE
            WHEN LOWER(SUBSTRING(url_parameters FROM 'utm_campaign=([^&]+)')) != 'nan' THEN LOWER(SUBSTRING(url_parameters FROM 'utm_campaign=([^&]+)'))
            ELSE null
        END AS utm_campaign,
        ROUND(SUM(spend)::NUMERIC, 3) AS total_spend,
        SUM(impressions) AS total_impressions,
        SUM(clicks) AS total_clicks,
        SUM(value) AS total_value,
        CASE
            WHEN SUM(impressions) = 0 THEN 0
            ELSE ROUND((SUM(clicks)::NUMERIC / SUM(impressions)), 3)
        END AS CTR,
        CASE
            WHEN SUM(clicks) = 0 THEN 0
            ELSE ROUND((SUM(spend)::NUMERIC / SUM(clicks)), 3)
        END AS CPC,
        CASE
            WHEN SUM(impressions) = 0 THEN 0
            ELSE ROUND((SUM(spend)::NUMERIC * 1000 / SUM(impressions)), 3)
        END AS CPM,
        CASE
            WHEN SUM(spend) = 0 THEN 0
            ELSE ROUND(((SUM(value)::NUMERIC - SUM(spend)) / SUM(spend) * 100), 3)
        END AS ROMI
    FROM gather
    GROUP BY ad_month, utm_campaign
),
with_previous AS (
    SELECT
        ad_month,
        utm_campaign,
        total_spend,
        total_impressions,
        total_clicks,
        total_value,
        CTR,
        CPC,
        CPM,
        ROMI,
        LAG(CPM) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_CPM,
        LAG(CTR) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_CTR,
        LAG(ROMI) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_ROMI
    FROM aggregated_data
)
SELECT
    ad_month,
    utm_campaign,
    total_spend,
    total_impressions,
    total_clicks,
    total_value,
    CTR,
    CPC,
    CPM,
    ROMI,
    ROUND(
        (CPM - prev_CPM) / NULLIF(prev_CPM, 0) * 100,
        2
    ) AS CPM_difference,
    ROUND(
        (CTR - prev_CTR) / NULLIF(prev_CTR, 0) * 100,
        2
    ) AS CTR_difference,
    ROUND(
        (ROMI - prev_ROMI) / NULLIF(prev_ROMI, 0) * 100,
        2
    ) AS ROMI_difference
FROM with_previous
ORDER BY utm_campaign, ad_month" (ad_month,utm_campaign,total_spend,total_impressions,total_clicks,total_value,ctr,cpc,cpm,romi,cpm_difference,ctr_difference,romi_difference) VALUES
	 ('2022-11-01','upgrade',0.000,0,0,0,0,0,0,0,-100.00,-100.00,-100.00),
	 ('2022-03-01','upgrades',52111.000,736199,9859,59025,0.013,5.286,70.784,13.268,NULL,NULL,NULL),
	 ('2022-04-01','upgrades',585380.000,5594394,37782,707409,0.007,15.494,104.637,20.846,47.83,-46.15,57.11),
	 ('2022-05-01','upgrades',811866.000,5216773,26418,945466,0.005,30.732,155.626,16.456,48.73,-28.57,-21.06),
	 ('2022-06-01','upgrades',445420.000,2352279,10662,547463,0.005,41.776,189.357,22.909,21.67,0.00,39.21),
	 ('2022-07-01','upgrades',12585.000,58167,314,17433,0.005,40.080,216.360,38.522,14.26,0.00,68.15),
	 ('2021-05-01','you_might_like',771561.000,3129660,36663,1072238,0.012,21.045,246.532,38.970,NULL,NULL,NULL),
	 ('2021-06-01','you_might_like',871164.000,2803179,33584,1114598,0.012,25.940,310.777,27.944,26.06,0.00,-28.29),
	 ('2021-07-01','you_might_like',1025687.000,3393030,38018,1257182,0.011,26.979,302.292,22.570,-2.73,-8.33,-19.23),
	 ('2021-08-01','you_might_like',699934.000,3014669,29010,861043,0.010,24.127,232.176,23.018,-23.19,-9.09,1.98);
INSERT INTO "WITH fb AS ( 
    SELECT
        ad_date,
        url_parameters,
        COALESCE(spend, 0) AS spend,
        COALESCE(impressions, 0) AS impressions,
        COALESCE(reach, 0) AS reach,
        COALESCE(clicks, 0) AS clicks,
        COALESCE(leads, 0) AS leads,
        COALESCE(value, 0) AS value
    FROM
        facebook_ads_basic_daily fabd
    LEFT JOIN facebook_campaign fc ON fabd.campaign_id = fc.campaign_id
    LEFT JOIN facebook_adset fa ON fabd.adset_id = fa.adset_id 
    UNION ALL
    SELECT
        ad_date,
        url_parameters,
        COALESCE(spend, 0) AS spend,
        COALESCE(impressions, 0) AS impressions,
        COALESCE(reach, 0) AS reach,
        COALESCE(clicks, 0) AS clicks,
        COALESCE(leads, 0) AS leads,
        COALESCE(value, 0) AS value
    FROM
        google_ads_basic_daily gabd
    WHERE 
        campaign_name IS NOT NULL
),
gather AS (
    SELECT * FROM fb
),
aggregated_data AS (
    SELECT
        DATE_TRUNC('month', ad_date)::date AS ad_month,
        CASE
            WHEN LOWER(SUBSTRING(url_parameters FROM 'utm_campaign=([^&]+)')) != 'nan' THEN LOWER(SUBSTRING(url_parameters FROM 'utm_campaign=([^&]+)'))
            ELSE null
        END AS utm_campaign,
        ROUND(SUM(spend)::NUMERIC, 3) AS total_spend,
        SUM(impressions) AS total_impressions,
        SUM(clicks) AS total_clicks,
        SUM(value) AS total_value,
        CASE
            WHEN SUM(impressions) = 0 THEN 0
            ELSE ROUND((SUM(clicks)::NUMERIC / SUM(impressions)), 3)
        END AS CTR,
        CASE
            WHEN SUM(clicks) = 0 THEN 0
            ELSE ROUND((SUM(spend)::NUMERIC / SUM(clicks)), 3)
        END AS CPC,
        CASE
            WHEN SUM(impressions) = 0 THEN 0
            ELSE ROUND((SUM(spend)::NUMERIC * 1000 / SUM(impressions)), 3)
        END AS CPM,
        CASE
            WHEN SUM(spend) = 0 THEN 0
            ELSE ROUND(((SUM(value)::NUMERIC - SUM(spend)) / SUM(spend) * 100), 3)
        END AS ROMI
    FROM gather
    GROUP BY ad_month, utm_campaign
),
with_previous AS (
    SELECT
        ad_month,
        utm_campaign,
        total_spend,
        total_impressions,
        total_clicks,
        total_value,
        CTR,
        CPC,
        CPM,
        ROMI,
        LAG(CPM) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_CPM,
        LAG(CTR) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_CTR,
        LAG(ROMI) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_ROMI
    FROM aggregated_data
)
SELECT
    ad_month,
    utm_campaign,
    total_spend,
    total_impressions,
    total_clicks,
    total_value,
    CTR,
    CPC,
    CPM,
    ROMI,
    ROUND(
        (CPM - prev_CPM) / NULLIF(prev_CPM, 0) * 100,
        2
    ) AS CPM_difference,
    ROUND(
        (CTR - prev_CTR) / NULLIF(prev_CTR, 0) * 100,
        2
    ) AS CTR_difference,
    ROUND(
        (ROMI - prev_ROMI) / NULLIF(prev_ROMI, 0) * 100,
        2
    ) AS ROMI_difference
FROM with_previous
ORDER BY utm_campaign, ad_month" (ad_month,utm_campaign,total_spend,total_impressions,total_clicks,total_value,ctr,cpc,cpm,romi,cpm_difference,ctr_difference,romi_difference) VALUES
	 ('2021-09-01','you_might_like',247816.000,864883,7187,322459,0.008,34.481,286.531,30.120,23.41,-20.00,30.85),
	 ('2021-10-01','you_might_like',341597.000,1069204,9502,436087,0.009,35.950,319.487,27.661,11.50,12.50,-8.16),
	 ('2021-11-01','you_might_like',306169.000,1046306,12725,388241,0.012,24.060,292.619,26.806,-8.41,33.33,-3.09),
	 ('2021-12-01','you_might_like',269337.000,685541,6450,334800,0.009,41.758,392.882,24.305,34.26,-25.00,-9.33),
	 ('2022-01-01','you_might_like',0.000,0,0,0,0,0,0,0,-100.00,-100.00,-100.00),
	 ('2021-05-01',NULL,75642.000,271204,3931,90574,0.014,19.242,278.912,19.740,NULL,NULL,NULL),
	 ('2021-06-01',NULL,206454.000,863073,12224,262220,0.014,16.889,239.208,27.011,-14.24,0.00,36.83),
	 ('2021-07-01',NULL,265918.000,1274975,16430,313275,0.013,16.185,208.567,17.809,-12.81,-7.14,-34.07),
	 ('2021-08-01',NULL,226860.000,1004106,16771,278291,0.017,13.527,225.932,22.671,8.33,30.77,27.30),
	 ('2021-09-01',NULL,89813.000,477972,7712,102801,0.016,11.646,187.904,14.461,-16.83,-5.88,-36.21);
INSERT INTO "WITH fb AS ( 
    SELECT
        ad_date,
        url_parameters,
        COALESCE(spend, 0) AS spend,
        COALESCE(impressions, 0) AS impressions,
        COALESCE(reach, 0) AS reach,
        COALESCE(clicks, 0) AS clicks,
        COALESCE(leads, 0) AS leads,
        COALESCE(value, 0) AS value
    FROM
        facebook_ads_basic_daily fabd
    LEFT JOIN facebook_campaign fc ON fabd.campaign_id = fc.campaign_id
    LEFT JOIN facebook_adset fa ON fabd.adset_id = fa.adset_id 
    UNION ALL
    SELECT
        ad_date,
        url_parameters,
        COALESCE(spend, 0) AS spend,
        COALESCE(impressions, 0) AS impressions,
        COALESCE(reach, 0) AS reach,
        COALESCE(clicks, 0) AS clicks,
        COALESCE(leads, 0) AS leads,
        COALESCE(value, 0) AS value
    FROM
        google_ads_basic_daily gabd
    WHERE 
        campaign_name IS NOT NULL
),
gather AS (
    SELECT * FROM fb
),
aggregated_data AS (
    SELECT
        DATE_TRUNC('month', ad_date)::date AS ad_month,
        CASE
            WHEN LOWER(SUBSTRING(url_parameters FROM 'utm_campaign=([^&]+)')) != 'nan' THEN LOWER(SUBSTRING(url_parameters FROM 'utm_campaign=([^&]+)'))
            ELSE null
        END AS utm_campaign,
        ROUND(SUM(spend)::NUMERIC, 3) AS total_spend,
        SUM(impressions) AS total_impressions,
        SUM(clicks) AS total_clicks,
        SUM(value) AS total_value,
        CASE
            WHEN SUM(impressions) = 0 THEN 0
            ELSE ROUND((SUM(clicks)::NUMERIC / SUM(impressions)), 3)
        END AS CTR,
        CASE
            WHEN SUM(clicks) = 0 THEN 0
            ELSE ROUND((SUM(spend)::NUMERIC / SUM(clicks)), 3)
        END AS CPC,
        CASE
            WHEN SUM(impressions) = 0 THEN 0
            ELSE ROUND((SUM(spend)::NUMERIC * 1000 / SUM(impressions)), 3)
        END AS CPM,
        CASE
            WHEN SUM(spend) = 0 THEN 0
            ELSE ROUND(((SUM(value)::NUMERIC - SUM(spend)) / SUM(spend) * 100), 3)
        END AS ROMI
    FROM gather
    GROUP BY ad_month, utm_campaign
),
with_previous AS (
    SELECT
        ad_month,
        utm_campaign,
        total_spend,
        total_impressions,
        total_clicks,
        total_value,
        CTR,
        CPC,
        CPM,
        ROMI,
        LAG(CPM) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_CPM,
        LAG(CTR) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_CTR,
        LAG(ROMI) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_ROMI
    FROM aggregated_data
)
SELECT
    ad_month,
    utm_campaign,
    total_spend,
    total_impressions,
    total_clicks,
    total_value,
    CTR,
    CPC,
    CPM,
    ROMI,
    ROUND(
        (CPM - prev_CPM) / NULLIF(prev_CPM, 0) * 100,
        2
    ) AS CPM_difference,
    ROUND(
        (CTR - prev_CTR) / NULLIF(prev_CTR, 0) * 100,
        2
    ) AS CTR_difference,
    ROUND(
        (ROMI - prev_ROMI) / NULLIF(prev_ROMI, 0) * 100,
        2
    ) AS ROMI_difference
FROM with_previous
ORDER BY utm_campaign, ad_month" (ad_month,utm_campaign,total_spend,total_impressions,total_clicks,total_value,ctr,cpc,cpm,romi,cpm_difference,ctr_difference,romi_difference) VALUES
	 ('2021-10-01',NULL,49634.000,231279,2233,58108,0.010,22.227,214.607,17.073,14.21,-37.50,18.06),
	 ('2021-12-01',NULL,1745.000,12690,24,1856,0.002,72.708,137.510,6.361,-35.92,-80.00,-62.74),
	 (NULL,NULL,0.000,0,0,0,0,0,0,0,-100.00,-100.00,-100.00);
