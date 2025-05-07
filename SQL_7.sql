WITH facebook_ads AS (
    SELECT
        fabd.ad_date,
        fabd.url_parameters,
        fabd.spend,
        fabd.impressions,
        fabd.reach,
        fabd.clicks,
        fabd.leads,
        fabd.value
    FROM facebook_ads_basic_daily fabd
    LEFT JOIN facebook_adset fa ON fa.adset_id = fabd.adset_id
    LEFT JOIN facebook_campaign fc ON fc.campaign_id = fabd.campaign_id
),
all_ads_data AS (
    SELECT
        ad_date,
        url_parameters,
        spend,
        impressions,
        reach,
        clicks,
        leads,
        value
    FROM google_ads_basic_daily
    UNION ALL
    SELECT
        ad_date,
        url_parameters,
        spend,
        impressions,
        reach,
        clicks,
        leads,
        value
    FROM facebook_ads
),
monthly_ads_data AS (
    SELECT
        to_char(DATE_TRUNC('month', ad_date),'YYY-MM-DD') AS ad_month,
        CASE
            WHEN LOWER(SUBSTRING(url_parameters FROM 'utm_campaign=([^&]*)')) = 'nan' THEN NULL
            ELSE LOWER(SUBSTRING(url_parameters FROM 'utm_campaign=([^&]*)'))
        END AS utm_campaign,
        SUM(COALESCE(spend, 0)) AS total_cost,
        SUM(COALESCE(impressions, 0)) AS total_impressions,
        SUM(COALESCE(clicks, 0)) AS total_clicks,
        SUM(COALESCE(value, 0)) AS total_value,
        CASE
            WHEN SUM(impressions) > 0 THEN ROUND(SUM(clicks) * 100.0 / SUM(impressions), 2)
            ELSE 0
        END AS ctr,
        CASE
            WHEN SUM(clicks) > 0 THEN ROUND(SUM(spend) / SUM(clicks), 2)
            ELSE 0
        END AS cpc,
        CASE
            WHEN SUM(impressions) > 0 THEN ROUND(1000 * SUM(spend) / SUM(impressions), 2)
            ELSE 0
        END AS cpm,
        CASE
            WHEN SUM(spend) > 0 THEN ROUND((SUM(value) - SUM(spend)) * 100.0 / SUM(spend), 2)
            ELSE 0
        END AS romi
    FROM all_ads_data
    GROUP BY ad_month, utm_campaign
),
monthly_comparison AS (
    SELECT
        mad.ad_month,
        mad.utm_campaign,
        mad.total_cost,
        mad.total_impressions,
        mad.total_clicks,
        mad.total_value,
        mad.ctr,
        mad.cpc,
        mad.cpm,
        mad.romi,
        LAG(mad.cpm) OVER (PARTITION BY mad.utm_campaign ORDER BY mad.ad_month) AS prev_cpm,
        LAG(mad.ctr) OVER (PARTITION BY mad.utm_campaign ORDER BY mad.ad_month) AS prev_ctr,
        LAG(mad.romi) OVER (PARTITION BY mad.utm_campaign ORDER BY mad.ad_month) AS prev_romi
    FROM monthly_ads_data mad
)
SELECT
    ad_month,
    utm_campaign,
    total_cost,
    total_impressions,
    total_clicks,
    total_value,
    ctr,
    cpc,
    cpm,
    romi,
    CASE
        WHEN prev_cpm IS NOT NULL AND prev_cpm != 0 THEN ROUND((cpm - prev_cpm) * 100.0 / prev_cpm, 2)
        ELSE NULL
    END AS cpm_change_percentage,
    CASE
        WHEN prev_ctr IS NOT NULL AND prev_ctr != 0 THEN ROUND((ctr - prev_ctr) * 100.0 / prev_ctr, 2)
        ELSE NULL
    END AS ctr_change_percentage,
    CASE
        WHEN prev_romi IS NOT NULL AND prev_romi != 0 THEN ROUND((romi - prev_romi) * 100.0 / prev_romi, 2)
        ELSE NULL
    END AS romi_change_percentage
FROM monthly_comparison
ORDER BY ad_month DESC, utm_campaign;
