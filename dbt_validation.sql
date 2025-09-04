with nofile as (
    select
        charge_ari
    from
        { { source(' chrono ', ' affirm_underwrites_user ') } }
    where
        event_time >= dateadd(day, -365, to_date(' { { start_date } } '))
        and decision_type in (' AFFIRM_GO_CREDIT ', ' LOAN_TERMS ')
        and fico is null
        and (
            credit_report_ari is null
            or credit_report_ari = ' none '
        )
    GROUP BY
        charge_ari
),
aa_prequal_card_status as (
    select
        apf.decision_id,
        apf.user_ari,
        pay_now_se.state_to
    from
        { { source(' dbt_analytics ', ' anywhere_prequal_fact ') } } apf
        left join { { source(' amplify_core ', ' carduserpaynowstateevent ') } } pay_now_se on apf.user_ari = pay_now_se.user_ari
        and pay_now_se.created <= apf.decision_created_dt
    where
        apf.decision_created_dt >= dateadd(day, -365, date(' { { start_date } } ')) qualify row_number() over (
            partition by apf.decision_id
            order by
                pay_now_se.created desc
        ) = 1
),
uw_signals as (
    with base as (
        select
            rodi.application_ari,
            e.decider_run_created_dt,
            e.affirm__user__exposure__v2,
            e.affirm__loan__minimum_exposure_remaining_amount_cents__v1,
            e.affirm__user__annual_income_dollars__v2,
            e.affirm__user__metropolitan_statistical_area__v1,
            e.credit_report__education__total_balance__v2
        from
            { { source(
                ' dbt_analytics__staging ',
                ' stg_base_applications_rodi '
            ) } } rodi
            left join { { source(
                ' dbt_analytics ',
                ' underwriting_deciderrun_event '
            ) } } e on e.decider_run_ari = rodi.decider_run_ari
            and e.decider_name = rodi.decision_type
        where
            e.decider_run_created_dt::date >= dateadd(day, -365, date(' { { start_date } } '))
    ),
    agg as (
        select
            application_ari,
            max(nvl(affirm__user__exposure__v2, 0)) / 100 as affirm_user_exposure_dollars,
            max(
                nvl(
                    affirm__loan__minimum_exposure_remaining_amount_cents__v1,
                    0
                )
            ) / 100 as affirm_loan_minimum_exposure_remaining_amount_dollars,
            max(nvl(affirm__user__annual_income_dollars__v2, 0)) as affirm_user_annual_income_dollars
        from
            base
        group by
            1
    ),
    latest_msa as (
        select
            application_ari,
            affirm__user__metropolitan_statistical_area__v1 as msa
        from
            base
        where
            affirm__user__metropolitan_statistical_area__v1 is not null qualify row_number() over (
                partition by application_ari
                order by
                    decider_run_created_dt desc
            ) = 1
    ),
    latest_student as (
        select
            application_ari,
            credit_report__education__total_balance__v2 as student_loan_balance
        from
            base
        where
            credit_report__education__total_balance__v2 is not null qualify row_number() over (
                partition by application_ari
                order by
                    decider_run_created_dt desc
            ) = 1
    )
    select
        agg.application_ari,
        lmsa.msa,
        lstu.student_loan_balance,
        agg.affirm_user_exposure_dollars,
        agg.affirm_loan_minimum_exposure_remaining_amount_dollars,
        agg.affirm_user_annual_income_dollars
    from
        agg
        left join latest_msa lmsa using (application_ari)
        left join latest_student lstu using (application_ari)
)
select
    nvl(cf.application_ari, pq.application_ari) as application_ari,
    nvl(cf.charge_ari, pq.charge_ari) as charge_ari,
    nvl(cf.prequal_ari, pq.prequal_ari) as prequal_ari,
    nvl(cf.checkout_ari, pq.checkout_ari) as checkout_ari,
    nvl(cf.user_ari, pq.user_ari) as user_ari,
    ud.user_id,
    ud.person_uuid,
    case
        when nvl(pq.start_date::date, cf.start_date::date) >= ' { { start_date } } ' then nvl(pq.start_date, cf.start_date)::date
        else null
    end as start_date,
    /* To avoid having partial data for funnel metrics before the start data pull date.
    Earlier data can happen for captured loans that were started earlier. */
    pq.start_date as prequal_start_date,
    cf.start_date as checkout_start_date,
    case
        when nvl(cf.captured_dt, cf.first_capture_dt)::date >= ' { { start_date } } ' then nvl(cf.captured_dt, cf.first_capture_dt)::date
        else null
    end as captured_date,
    cf.captured_dt,
    cf.first_capture_dt,
    case
        when nvl(pq.is_applied, 0) > 0
        and cf.prequal_ari is null then ' PQ Funnel / No Checkout Yet '
        when cf.prequal_ari is not null then ' PQ @ Checkout Funnel '
        else ' non PQ @ Checkout Funnel '
    end as funnel_stage,
    case
        when upre.creation_type = ' proactive ' then ' PMPQ '
        else ' other '
    end as pmpq_flag,
    -- application_level_repeat_flag
    case
        when nvl(pq.start_date, cf.checkout_created_dt) > nvl(
            fpl.PERSON_FIRST_CAPTURED_CHARGE_CREATED_DT,
            current_date + 1
        ) then ' Repeat '
        else ' NTA '
    end as repeat_flag,
    -- application_level_amazon_ntp_flag
    case
        when nvl(pq.merchant_ari, cf.merchant_ari) not in (' R8P0IVNYV3G2CNIT ') then ' non Amazon '
        when nvl(pq.user_ari, cf.user_ari) is not null
        and nvl(pq.start_date, cf.checkout_created_dt) <= coalesce(
            fpl.PERSON_FIRST_MERCHANT_CAPTURED_DT_RUFUS,
            current_date + 1
        ) then ' Amazon NTP '
        when nvl(pq.user_ari, cf.user_ari) is not null
        and nvl(pq.start_date, cf.checkout_created_dt) > coalesce(
            fpl.PERSON_FIRST_MERCHANT_CAPTURED_DT_RUFUS,
            current_date + 1
        ) then ' Amazon Repeat '
        when nvl(pq.user_ari, cf.user_ari) is null then ' Unknown '
        else ' Unknown '
    end as amazon_ntp_flag,
    months_between(
        nvl(pq.start_date, cf.start_date)::date,
        nvl(
            fpl.person_first_captured_charge_created_dt,
            nvl(pq.start_date, cf.start_date)
        )::date
    ) as user_tenured_month,
    case
        when coalesce(cf.is_checkout_applied, pq.is_applied, 0) > 0 then 1
        else 0
    end as is_applied,
    -- is_approved_logic
    case
        when nvl(pq.merchant_ari, cf.merchant_ari) in (' R8P0IVNYV3G2CNIT ') then case
            when cf.financing_type1 ilike ' % split_pay % '
            and cf.total_amount <= 750
            and cf.start_date <= ' 2023 -07 -01 ' then nvl(cf.is_approved_installments, 0)
            when cf.financing_type1 ilike ' % split_pay % '
            and cf.total_amount <= 500
            and nvl(pq.start_date, cf.start_date) <= ' 2024 -11 -01 ' then nvl(cf.is_approved_split_pay, 0)
            when cf.financing_type1 ilike ' % split_pay % '
            and cf.total_amount <= 500
            and nvl(pq.start_date, cf.start_date) > ' 2024 -11 -01 ' then nvl(cf.is_approved_split_pay, 0) * nvl(cf.is_approved_installments, 0)
            else GREATEST(
                NVL(
                    cf.is_approved,
                    CASE
                        WHEN pq.decision_state = ' approved ' THEN 1
                        ELSE 0
                    END
                ),
                0
            )
        end
        else GREATEST(
            NVL(
                cf.is_approved,
                CASE
                    WHEN pq.decision_state = ' approved ' THEN 1
                    ELSE 0
                END
            ),
            0
        )
    end as is_approved,
    case
        when nvl(cf.is_approved_installments, 0) = 1 then cf.declined_reason_il
        when nvl(cf.is_approved_split_pay, 0) = 1 then cf.declined_reason_sp
        else nvl(cf.declined_reason, pq.declined_reason)
    end as decline_reason,
    nvl(cf.is_checkout_applied, 0) as is_checkout_applied,
    -- is_cf_approved_logic
    case
        when cf.application_ari is null then null
        when cf.merchant_ari in (' R8P0IVNYV3G2CNIT ') then case
            when cf.financing_type1 ilike ' % split_pay % '
            and cf.total_amount <= 750
            and cf.start_date <= ' 2023 -07 -01 ' then nvl(cf.is_approved_installments, 0)
            when cf.financing_type1 ilike ' % split_pay % '
            and cf.total_amount <= 500
            and cf.start_date <= ' 2024 -11 -01 ' then nvl(cf.is_approved_split_pay, 0)
            when cf.financing_type1 ilike ' % split_pay % '
            and cf.total_amount <= 500
            and cf.start_date > ' 2024 -11 -01 ' then nvl(cf.is_approved_split_pay, 0) * nvl(cf.is_approved_installments, 0)
            else nvl(cf.is_approved, 0)
        end
        else nvl(cf.is_approved, 0)
    end as is_checkout_approved,
    cf.is_approved_installments as is_checkout_il_approved,
    cf.is_approved_split_pay as is_checkout_sp_approved,
    nvl(cf.is_confirmed, 0) as is_confirmed,
    nvl(cf.is_authed, 0) as is_authed,
    nvl(cf.is_merchant_captured, 0) as is_merchant_captured,
    nvl(cf.is_captured, 0) as is_captured,
    case
        when coft.offered_loan_type1_chargedetails is not null
        and coft.offered_loan_type2_chargedetails is not null
        and coft.offered_loan_type3_chargedetails is not null then ' 3 '
        when coft.offered_loan_type1_chargedetails is not null
        and coft.offered_loan_type2_chargedetails is not null then ' 2 '
        when coft.offered_loan_type1_chargedetails is not null then ' 1 '
        when nvl(pq.is_applied, 0) > 0
        and cf.prequal_ari is null then ' PQ Funnel / No Checkout Yet '
        else ' No offer '
    end as number_of_offers,
    -- captured_level_product
    case
        when nvl(cf.merchant_ari, pq.merchant_ari) in (
            ' 8L2VTJ7XV2QQ4PCU ',
            ' HVKQRIE7X73QD510 ',
            ' B7P1CU8QAXPM00LV ',
            ' 42D1KYQZVNLH6HUS '
        ) then ' Peloton '
        when nvl(cf.merchant_ari, pq.merchant_ari) in (
            ' 5CHTV8MOBVHDJRYD ',
            ' S42ODR06M6TG0JK0 ',
            ' MU6E2PSKFC7BGCTG '
        ) then ' Target '
        when nvl(cf.merchant_ari, pq.merchant_ari) in (' 456UKJA79KGSO1TZ ', ' X96ZPHNIR5GRHG1W ') then ' Walmart '
        when nvl(cf.merchant_ari, pq.merchant_ari) in (' R8P0IVNYV3G2CNIT ') then ' Amazon '
        when checkout_mart.IS_PBA_SLINGSHOT_CHECKOUT = 1
        and checkout_mart.is_adaptive = 1 then ' Shopify '
        when cf.checkout_flow_type = ' shopify_affirm_go_v3 ' then ' Shopify '
        when (
            cf.guarantee_ari is not null
            and ud.label = ' affirm '
        )
        or (
            mdim.MERCHANT_PRODUCT_TYPE = ' Affirm Anywhere '
            or mdim.merchant_ari in (
                ' 4B1SGJYYDNJIXA3H ',
                ' ST8FQHFDIUWLVVQJ ',
                ' CH89JWTJBIWF8V1E ',
                ' WUSUUM3PA2YHQNHM '
            )
        ) then (
            case
                when (pq_card_label.state_to is not null)
                or (
                    ud.is_debit_plus_onboarded = 1
                    and ud.first_debit_onboarding_dt < apf.decision_created_dt
                ) then ' Affirm Card '
                else ' Affirm Anywhere '
            end
        )
        else ' General Core & AC '
    end as application_product_category,
    case
        when nvl(cf.is_captured, 0) = 1 then (
            case
                when (
                    case
                        when (
                            cf.guarantee_ari is not null
                            and aip.checkout_ari is not null
                        )
                        or (
                            cf.merchant_ari in (
                                ' 4B1SGJYYDNJIXA3H ',
                                ' ST8FQHFDIUWLVVQJ ',
                                ' CH89JWTJBIWF8V1E ',
                                ' WUSUUM3PA2YHQNHM '
                            )
                        ) then case
                            -- Affirm Card
                            when (
                                (
                                    cm.product_platform = ' Anywhere '
                                    and chfc.checkout_issuing_instrument_type = ' debit_plus '
                                )
                                or cm.loan_product_group like ' Debit + % '
                            ) then (
                                case
                                    when cf.plan_frequency = ' daily '
                                    and cf.plan_interval = 30 then ' Affirm Card PiX '
                                    when cf.plan_frequency = ' daily '
                                    and cf.plan_interval = 15 then ' Affirm Card PiX '
                                    when cf.plan_frequency = ' weekly '
                                    and cf.loan_type = ' classic ' then ' Affirm Card PiX '
                                    when cf.plan_frequency = ' weekly '
                                    or cf.loan_type = ' affirm_go_v3 ' then ' Affirm Card PiX '
                                    when cf.loan_type = ' classic '
                                    or cf.plan_frequency = ' monthly ' then ' Affirm Card IL '
                                    else ' Affirm Card Other '
                                end
                            )
                            -- Affirm Anywhere
                            when cf.plan_frequency = ' daily '
                            and cf.plan_interval = 30 then ' AA PiX '
                            when cf.plan_frequency = ' daily '
                            and cf.plan_interval = 15 then ' AA PiX '
                            when cf.plan_frequency = ' weekly '
                            and cf.loan_type = ' classic ' then ' AA PiX '
                            when cf.plan_frequency = ' weekly '
                            or cf.loan_type = ' affirm_go_v3 ' then ' AA PiX '
                            when cf.loan_type = ' classic '
                            or cf.plan_frequency = ' monthly ' then ' AA IL '
                            else ' AA Other '
                        end
                        else ' other '
                    end
                ) in (' AA IL ', ' Affirm Card IL ') then ' AA & Card IL '
                when (
                    case
                        when (
                            cf.guarantee_ari is not null
                            and aip.checkout_ari is not null
                        )
                        or (
                            cf.merchant_ari in (
                                ' 4B1SGJYYDNJIXA3H ',
                                ' ST8FQHFDIUWLVVQJ ',
                                ' CH89JWTJBIWF8V1E ',
                                ' WUSUUM3PA2YHQNHM '
                            )
                        ) then case
                            -- Affirm Card
                            when (
                                (
                                    cm.product_platform = ' Anywhere '
                                    and chfc.checkout_issuing_instrument_type = ' debit_plus '
                                )
                                or cm.loan_product_group like ' Debit + % '
                            ) then (
                                case
                                    when cf.plan_frequency = ' daily '
                                    and cf.plan_interval = 30 then ' Affirm Card PiX '
                                    when cf.plan_frequency = ' daily '
                                    and cf.plan_interval = 15 then ' Affirm Card PiX '
                                    when cf.plan_frequency = ' weekly '
                                    and cf.loan_type = ' classic ' then ' Affirm Card PiX '
                                    when cf.plan_frequency = ' weekly '
                                    or cf.loan_type = ' affirm_go_v3 ' then ' Affirm Card PiX '
                                    when cf.loan_type = ' classic '
                                    or cf.plan_frequency = ' monthly ' then ' Affirm Card IL '
                                    else ' Affirm Card Other '
                                end
                            )
                            -- Affirm Anywhere
                            when cf.plan_frequency = ' daily '
                            and cf.plan_interval = 30 then ' AA PiX '
                            when cf.plan_frequency = ' daily '
                            and cf.plan_interval = 15 then ' AA PiX '
                            when cf.plan_frequency = ' weekly '
                            and cf.loan_type = ' classic ' then ' AA PiX '
                            when cf.plan_frequency = ' weekly '
                            or cf.loan_type = ' affirm_go_v3 ' then ' AA PiX '
                            when cf.loan_type = ' classic '
                            or cf.plan_frequency = ' monthly ' then ' AA IL '
                            else ' AA Other '
                        end
                        else ' other '
                    end
                ) in (' AA PiX ', ' Affirm Card PiX ') then ' AA & Card PiX '
                when (
                    case
                        when (
                            cf.guarantee_ari is not null
                            and aip.checkout_ari is not null
                        )
                        or (
                            cf.merchant_ari in (
                                ' 4B1SGJYYDNJIXA3H ',
                                ' ST8FQHFDIUWLVVQJ ',
                                ' CH89JWTJBIWF8V1E ',
                                ' WUSUUM3PA2YHQNHM '
                            )
                        ) then case
                            -- Affirm Card
                            when (
                                (
                                    cm.product_platform = ' Anywhere '
                                    and chfc.checkout_issuing_instrument_type = ' debit_plus '
                                )
                                or cm.loan_product_group like ' Debit + % '
                            ) then (
                                case
                                    when cf.plan_frequency = ' daily '
                                    and cf.plan_interval = 30 then ' Affirm Card PiX '
                                    when cf.plan_frequency = ' daily '
                                    and cf.plan_interval = 15 then ' Affirm Card PiX '
                                    when cf.plan_frequency = ' weekly '
                                    and cf.loan_type = ' classic ' then ' Affirm Card PiX '
                                    when cf.plan_frequency = ' weekly '
                                    or cf.loan_type = ' affirm_go_v3 ' then ' Affirm Card PiX '
                                    when cf.loan_type = ' classic '
                                    or cf.plan_frequency = ' monthly ' then ' Affirm Card IL '
                                    else ' Affirm Card Other '
                                end
                            )
                            -- Affirm Anywhere
                            when cf.plan_frequency = ' daily '
                            and cf.plan_interval = 30 then ' AA PiX '
                            when cf.plan_frequency = ' daily '
                            and cf.plan_interval = 15 then ' AA PiX '
                            when cf.plan_frequency = ' weekly '
                            and cf.loan_type = ' classic ' then ' AA PiX '
                            when cf.plan_frequency = ' weekly '
                            or cf.loan_type = ' affirm_go_v3 ' then ' AA PiX '
                            when cf.loan_type = ' classic '
                            or cf.plan_frequency = ' monthly ' then ' AA IL '
                            else ' AA Other '
                        end
                        else ' other '
                    end
                ) not in (' other ') then ' AA & Card other '
                when checkout_mart.IS_PBA_SLINGSHOT_CHECKOUT = 1 then case
                    when cf.plan_frequency = ' daily '
                    and cf.plan_interval = 30 then ' Shopify PiX '
                    when cf.plan_frequency = ' daily '
                    and cf.plan_interval = 15 then ' Shopify PiX '
                    when cf.plan_frequency = ' weekly '
                    and cf.loan_type = ' classic ' then ' Shopify PiX '
                    when cf.plan_frequency = ' weekly '
                    or cf.loan_type = ' affirm_go_v3 ' then ' Shopify PiX '
                    else ' Shopify IL '
                end
                when cf.plan_frequency = ' daily '
                and cf.plan_interval = 30 then ' Core PiX '
                when cf.plan_frequency = ' daily '
                and cf.plan_interval = 15 then ' Core PiX '
                when cf.plan_frequency = ' weekly '
                and cf.loan_type = ' classic ' then ' Core PiX '
                when cf.checkout_flow_type in (' affirm_go ', ' split_pay_go_v3 ')
                or cf.loan_type = ' affirm_go_v3 '
                or cf.plan_frequency = ' weekly ' then ' Core PiX '
                else ' Core IL '
            end
        )
        else ' not captured '
    end as captured_product_category,
    -- captured_level_subproduct
    case
        when nvl(cf.is_captured, 0) = 0 then ' not captured '
        when (
            case
                when (
                    cf.guarantee_ari is not null
                    and aip.checkout_ari is not null
                )
                or (
                    cf.merchant_ari in (
                        ' 4B1SGJYYDNJIXA3H ',
                        ' ST8FQHFDIUWLVVQJ ',
                        ' CH89JWTJBIWF8V1E ',
                        ' WUSUUM3PA2YHQNHM '
                    )
                ) then case
                    -- Affirm Card
                    when (
                        (
                            cm.product_platform = ' Anywhere '
                            and chfc.checkout_issuing_instrument_type = ' debit_plus '
                        )
                        or cm.loan_product_group like ' Debit + % '
                    ) then (
                        case
                            when cf.plan_frequency = ' daily '
                            and cf.plan_interval = 30 then ' Affirm Card PiX '
                            when cf.plan_frequency = ' daily '
                            and cf.plan_interval = 15 then ' Affirm Card PiX '
                            when cf.plan_frequency = ' weekly '
                            and cf.loan_type = ' classic ' then ' Affirm Card PiX '
                            when cf.plan_frequency = ' weekly '
                            or cf.loan_type = ' affirm_go_v3 ' then ' Affirm Card PiX '
                            when cf.loan_type = ' classic '
                            or cf.plan_frequency = ' monthly ' then ' Affirm Card IL '
                            else ' Affirm Card Other '
                        end
                    )
                    -- Affirm Anywhere
                    when cf.plan_frequency = ' daily '
                    and cf.plan_interval = 30 then ' AA PiX '
                    when cf.plan_frequency = ' daily '
                    and cf.plan_interval = 15 then ' AA PiX '
                    when cf.plan_frequency = ' weekly '
                    and cf.loan_type = ' classic ' then ' AA PiX '
                    when cf.plan_frequency = ' weekly '
                    or cf.loan_type = ' affirm_go_v3 ' then ' AA PiX '
                    when cf.loan_type = ' classic '
                    or cf.plan_frequency = ' monthly ' then ' AA IL '
                    else ' AA Other '
                end
                else ' other '
            end
        ) not in (' other ') then case
            when cf.plan_frequency = ' daily '
            and cf.plan_interval = 30 then ' AA & Card Pi30 '
            when cf.plan_frequency = ' daily '
            and cf.plan_interval = 15 then ' AA & Card Pi2 '
            when cf.plan_frequency = ' weekly '
            and cf.loan_type = ' classic ' then ' AA & Card 4BW '
            when cf.plan_frequency = ' weekly '
            or cf.loan_type = ' affirm_go_v3 ' then ' AA & Card Pi4 '
            else ' AA & Card IL '
        end
        when checkout_mart.IS_PBA_SLINGSHOT_CHECKOUT = 1 then case
            when cf.plan_frequency = ' daily '
            and cf.plan_interval = 30 then ' Shopify Pi30 '
            when cf.plan_frequency = ' daily '
            and cf.plan_interval = 15 then ' Shopify Pi2 '
            when cf.plan_frequency = ' weekly '
            and cf.loan_type = ' classic ' then ' Shopify 4BW '
            when cf.plan_frequency = ' weekly '
            or cf.loan_type = ' affirm_go_v3 ' then ' Shopify Pi4 '
            else ' Shopify IL '
        end
        when cf.plan_frequency = ' daily '
        and cf.plan_interval = 30 then ' Core Pi30 '
        when cf.plan_frequency = ' daily '
        and cf.plan_interval = 15 then ' Core Pi2 '
        when cf.plan_frequency = ' weekly '
        and cf.loan_type = ' classic ' then ' Core 4BW '
        when cf.checkout_flow_type in (' affirm_go ', ' split_pay_go_v3 ')
        or cf.loan_type = ' affirm_go_v3 '
        or cf.plan_frequency = ' weekly ' then ' Core Pi4 '
        else ' Core IL '
    end as captured_product_subcategory,
    -- core_index
    case
        when case
            when nvl(cf.is_captured, 0) = 1 then (
                case
                    when (
                        case
                            when (
                                cf.guarantee_ari is not null
                                and aip.checkout_ari is not null
                            )
                            or (
                                cf.merchant_ari in (
                                    ' 4B1SGJYYDNJIXA3H ',
                                    ' ST8FQHFDIUWLVVQJ ',
                                    ' CH89JWTJBIWF8V1E ',
                                    ' WUSUUM3PA2YHQNHM '
                                )
                            ) then case
                                -- Affirm Card
                                when (
                                    (
                                        cm.product_platform = ' Anywhere '
                                        and chfc.checkout_issuing_instrument_type = ' debit_plus '
                                    )
                                    or cm.loan_product_group like ' Debit + % '
                                ) then (
                                    case
                                        when cf.plan_frequency = ' daily '
                                        and cf.plan_interval = 30 then ' Affirm Card PiX '
                                        when cf.plan_frequency = ' daily '
                                        and cf.plan_interval = 15 then ' Affirm Card PiX '
                                        when cf.plan_frequency = ' weekly '
                                        and cf.loan_type = ' classic ' then ' Affirm Card PiX '
                                        when cf.plan_frequency = ' weekly '
                                        or cf.loan_type = ' affirm_go_v3 ' then ' Affirm Card PiX '
                                        when cf.loan_type = ' classic '
                                        or cf.plan_frequency = ' monthly ' then ' Affirm Card IL '
                                        else ' Affirm Card Other '
                                    end
                                )
                                -- Affirm Anywhere
                                when cf.plan_frequency = ' daily '
                                and cf.plan_interval = 30 then ' AA PiX '
                                when cf.plan_frequency = ' daily '
                                and cf.plan_interval = 15 then ' AA PiX '
                                when cf.plan_frequency = ' weekly '
                                and cf.loan_type = ' classic ' then ' AA PiX '
                                when cf.plan_frequency = ' weekly '
                                or cf.loan_type = ' affirm_go_v3 ' then ' AA PiX '
                                when cf.loan_type = ' classic '
                                or cf.plan_frequency = ' monthly ' then ' AA IL '
                                else ' AA Other '
                            end
                            else ' other '
                        end
                    ) in (' AA IL ', ' Affirm Card IL ') then ' AA & Card IL '
                    when (
                        case
                            when (
                                cf.guarantee_ari is not null
                                and aip.checkout_ari is not null
                            )
                            or (
                                cf.merchant_ari in (
                                    ' 4B1SGJYYDNJIXA3H ',
                                    ' ST8FQHFDIUWLVVQJ ',
                                    ' CH89JWTJBIWF8V1E ',
                                    ' WUSUUM3PA2YHQNHM '
                                )
                            ) then case
                                -- Affirm Card
                                when (
                                    (
                                        cm.product_platform = ' Anywhere '
                                        and chfc.checkout_issuing_instrument_type = ' debit_plus '
                                    )
                                    or cm.loan_product_group like ' Debit + % '
                                ) then (
                                    case
                                        when cf.plan_frequency = ' daily '
                                        and cf.plan_interval = 30 then ' Affirm Card PiX '
                                        when cf.plan_frequency = ' daily '
                                        and cf.plan_interval = 15 then ' Affirm Card PiX '
                                        when cf.plan_frequency = ' weekly '
                                        and cf.loan_type = ' classic ' then ' Affirm Card PiX '
                                        when cf.plan_frequency = ' weekly '
                                        or cf.loan_type = ' affirm_go_v3 ' then ' Affirm Card PiX '
                                        when cf.loan_type = ' classic '
                                        or cf.plan_frequency = ' monthly ' then ' Affirm Card IL '
                                        else ' Affirm Card Other '
                                    end
                                )
                                -- Affirm Anywhere
                                when cf.plan_frequency = ' daily '
                                and cf.plan_interval = 30 then ' AA PiX '
                                when cf.plan_frequency = ' daily '
                                and cf.plan_interval = 15 then ' AA PiX '
                                when cf.plan_frequency = ' weekly '
                                and cf.loan_type = ' classic ' then ' AA PiX '
                                when cf.plan_frequency = ' weekly '
                                or cf.loan_type = ' affirm_go_v3 ' then ' AA PiX '
                                when cf.loan_type = ' classic '
                                or cf.plan_frequency = ' monthly ' then ' AA IL '
                                else ' AA Other '
                            end
                            else ' other '
                        end
                    ) in (' AA PiX ', ' Affirm Card PiX ') then ' AA & Card PiX '
                    when (
                        case
                            when (
                                cf.guarantee_ari is not null
                                and aip.checkout_ari is not null
                            )
                            or (
                                cf.merchant_ari in (
                                    ' 4B1SGJYYDNJIXA3H ',
                                    ' ST8FQHFDIUWLVVQJ ',
                                    ' CH89JWTJBIWF8V1E ',
                                    ' WUSUUM3PA2YHQNHM '
                                )
                            ) then case
                                -- Affirm Card
                                when (
                                    (
                                        cm.product_platform = ' Anywhere '
                                        and chfc.checkout_issuing_instrument_type = ' debit_plus '
                                    )
                                    or cm.loan_product_group like ' Debit + % '
                                ) then (
                                    case
                                        when cf.plan_frequency = ' daily '
                                        and cf.plan_interval = 30 then ' Affirm Card PiX '
                                        when cf.plan_frequency = ' daily '
                                        and cf.plan_interval = 15 then ' Affirm Card PiX '
                                        when cf.plan_frequency = ' weekly '
                                        and cf.loan_type = ' classic ' then ' Affirm Card PiX '
                                        when cf.plan_frequency = ' weekly '
                                        or cf.loan_type = ' affirm_go_v3 ' then ' Affirm Card PiX '
                                        when cf.loan_type = ' classic '
                                        or cf.plan_frequency = ' monthly ' then ' Affirm Card IL '
                                        else ' Affirm Card Other '
                                    end
                                )
                                -- Affirm Anywhere
                                when cf.plan_frequency = ' daily '
                                and cf.plan_interval = 30 then ' AA PiX '
                                when cf.plan_frequency = ' daily '
                                and cf.plan_interval = 15 then ' AA PiX '
                                when cf.plan_frequency = ' weekly '
                                and cf.loan_type = ' classic ' then ' AA PiX '
                                when cf.plan_frequency = ' weekly '
                                or cf.loan_type = ' affirm_go_v3 ' then ' AA PiX '
                                when cf.loan_type = ' classic '
                                or cf.plan_frequency = ' monthly ' then ' AA IL '
                                else ' AA Other '
                            end
                            else ' other '
                        end
                    ) not in (' other ') then ' AA & Card other '
                    when checkout_mart.IS_PBA_SLINGSHOT_CHECKOUT = 1 then case
                        when cf.plan_frequency = ' daily '
                        and cf.plan_interval = 30 then ' Shopify PiX '
                        when cf.plan_frequency = ' daily '
                        and cf.plan_interval = 15 then ' Shopify PiX '
                        when cf.plan_frequency = ' weekly '
                        and cf.loan_type = ' classic ' then ' Shopify PiX '
                        when cf.plan_frequency = ' weekly '
                        or cf.loan_type = ' affirm_go_v3 ' then ' Shopify PiX '
                        else ' Shopify IL '
                    end
                    when cf.plan_frequency = ' daily '
                    and cf.plan_interval = 30 then ' Core PiX '
                    when cf.plan_frequency = ' daily '
                    and cf.plan_interval = 15 then ' Core PiX '
                    when cf.plan_frequency = ' weekly '
                    and cf.loan_type = ' classic ' then ' Core PiX '
                    when cf.checkout_flow_type in (' affirm_go ', ' split_pay_go_v3 ')
                    or cf.loan_type = ' affirm_go_v3 '
                    or cf.plan_frequency = ' weekly ' then ' Core PiX '
                    else ' Core IL '
                end
            )
            else ' not captured '
        end in (' Core IL ', ' Shopify IL ') then 1
        else 0
    end as core_index,
    -- captured_merchant_definition
    case
        when cf.merchant_ari in (
            ' 8L2VTJ7XV2QQ4PCU ',
            ' HVKQRIE7X73QD510 ',
            ' B7P1CU8QAXPM00LV ',
            ' 42D1KYQZVNLH6HUS '
        ) then ' Peloton '
        when cf.merchant_ari in (
            ' 5CHTV8MOBVHDJRYD ',
            ' S42ODR06M6TG0JK0 ',
            ' MU6E2PSKFC7BGCTG '
        ) then ' Target '
        when cf.merchant_ari in (' 456UKJA79KGSO1TZ ', ' X96ZPHNIR5GRHG1W ') then ' Walmart '
        when cf.merchant_ari in (' R8P0IVNYV3G2CNIT ') then ' Amazon '
        when (
            case
                when nvl(cf.is_captured, 0) = 1 then (
                    case
                        when (
                            case
                                when (
                                    cf.guarantee_ari is not null
                                    and aip.checkout_ari is not null
                                )
                                or (
                                    cf.merchant_ari in (
                                        ' 4B1SGJYYDNJIXA3H ',
                                        ' ST8FQHFDIUWLVVQJ ',
                                        ' CH89JWTJBIWF8V1E ',
                                        ' WUSUUM3PA2YHQNHM '
                                    )
                                ) then case
                                    -- Affirm Card
                                    when (
                                        (
                                            cm.product_platform = ' Anywhere '
                                            and chfc.checkout_issuing_instrument_type = ' debit_plus '
                                        )
                                        or cm.loan_product_group like ' Debit + % '
                                    ) then (
                                        case
                                            when cf.plan_frequency = ' daily '
                                            and cf.plan_interval = 30 then ' Affirm Card PiX '
                                            when cf.plan_frequency = ' daily '
                                            and cf.plan_interval = 15 then ' Affirm Card PiX '
                                            when cf.plan_frequency = ' weekly '
                                            and cf.loan_type = ' classic ' then ' Affirm Card PiX '
                                            when cf.plan_frequency = ' weekly '
                                            or cf.loan_type = ' affirm_go_v3 ' then ' Affirm Card PiX '
                                            when cf.loan_type = ' classic '
                                            or cf.plan_frequency = ' monthly ' then ' Affirm Card IL '
                                            else ' Affirm Card Other '
                                        end
                                    )
                                    -- Affirm Anywhere
                                    when cf.plan_frequency = ' daily '
                                    and cf.plan_interval = 30 then ' AA PiX '
                                    when cf.plan_frequency = ' daily '
                                    and cf.plan_interval = 15 then ' AA PiX '
                                    when cf.plan_frequency = ' weekly '
                                    and cf.loan_type = ' classic ' then ' AA PiX '
                                    when cf.plan_frequency = ' weekly '
                                    or cf.loan_type = ' affirm_go_v3 ' then ' AA PiX '
                                    when cf.loan_type = ' classic '
                                    or cf.plan_frequency = ' monthly ' then ' AA IL '
                                    else ' AA Other '
                                end
                                else ' other '
                            end
                        ) in (' AA IL ', ' Affirm Card IL ') then ' AA & Card IL '
                        when (
                            case
                                when (
                                    cf.guarantee_ari is not null
                                    and aip.checkout_ari is not null
                                )
                                or (
                                    cf.merchant_ari in (
                                        ' 4B1SGJYYDNJIXA3H ',
                                        ' ST8FQHFDIUWLVVQJ ',
                                        ' CH89JWTJBIWF8V1E ',
                                        ' WUSUUM3PA2YHQNHM '
                                    )
                                ) then case
                                    -- Affirm Card
                                    when (
                                        (
                                            cm.product_platform = ' Anywhere '
                                            and chfc.checkout_issuing_instrument_type = ' debit_plus '
                                        )
                                        or cm.loan_product_group like ' Debit + % '
                                    ) then (
                                        case
                                            when cf.plan_frequency = ' daily '
                                            and cf.plan_interval = 30 then ' Affirm Card PiX '
                                            when cf.plan_frequency = ' daily '
                                            and cf.plan_interval = 15 then ' Affirm Card PiX '
                                            when cf.plan_frequency = ' weekly '
                                            and cf.loan_type = ' classic ' then ' Affirm Card PiX '
                                            when cf.plan_frequency = ' weekly '
                                            or cf.loan_type = ' affirm_go_v3 ' then ' Affirm Card PiX '
                                            when cf.loan_type = ' classic '
                                            or cf.plan_frequency = ' monthly ' then ' Affirm Card IL '
                                            else ' Affirm Card Other '
                                        end
                                    )
                                    -- Affirm Anywhere
                                    when cf.plan_frequency = ' daily '
                                    and cf.plan_interval = 30 then ' AA PiX '
                                    when cf.plan_frequency = ' daily '
                                    and cf.plan_interval = 15 then ' AA PiX '
                                    when cf.plan_frequency = ' weekly '
                                    and cf.loan_type = ' classic ' then ' AA PiX '
                                    when cf.plan_frequency = ' weekly '
                                    or cf.loan_type = ' affirm_go_v3 ' then ' AA PiX '
                                    when cf.loan_type = ' classic '
                                    or cf.plan_frequency = ' monthly ' then ' AA IL '
                                    else ' AA Other '
                                end
                                else ' other '
                            end
                        ) in (' AA PiX ', ' Affirm Card PiX ') then ' AA & Card PiX '
                        when (
                            case
                                when (
                                    cf.guarantee_ari is not null
                                    and aip.checkout_ari is not null
                                )
                                or (
                                    cf.merchant_ari in (
                                        ' 4B1SGJYYDNJIXA3H ',
                                        ' ST8FQHFDIUWLVVQJ ',
                                        ' CH89JWTJBIWF8V1E ',
                                        ' WUSUUM3PA2YHQNHM '
                                    )
                                ) then case
                                    -- Affirm Card
                                    when (
                                        (
                                            cm.product_platform = ' Anywhere '
                                            and chfc.checkout_issuing_instrument_type = ' debit_plus '
                                        )
                                        or cm.loan_product_group like ' Debit + % '
                                    ) then (
                                        case
                                            when cf.plan_frequency = ' daily '
                                            and cf.plan_interval = 30 then ' Affirm Card PiX '
                                            when cf.plan_frequency = ' daily '
                                            and cf.plan_interval = 15 then ' Affirm Card PiX '
                                            when cf.plan_frequency = ' weekly '
                                            and cf.loan_type = ' classic ' then ' Affirm Card PiX '
                                            when cf.plan_frequency = ' weekly '
                                            or cf.loan_type = ' affirm_go_v3 ' then ' Affirm Card PiX '
                                            when cf.loan_type = ' classic '
                                            or cf.plan_frequency = ' monthly ' then ' Affirm Card IL '
                                            else ' Affirm Card Other '
                                        end
                                    )
                                    -- Affirm Anywhere
                                    when cf.plan_frequency = ' daily '
                                    and cf.plan_interval = 30 then ' AA PiX '
                                    when cf.plan_frequency = ' daily '
                                    and cf.plan_interval = 15 then ' AA PiX '
                                    when cf.plan_frequency = ' weekly '
                                    and cf.loan_type = ' classic ' then ' AA PiX '
                                    when cf.plan_frequency = ' weekly '
                                    or cf.loan_type = ' affirm_go_v3 ' then ' AA PiX '
                                    when cf.loan_type = ' classic '
                                    or cf.plan_frequency = ' monthly ' then ' AA IL '
                                    else ' AA Other '
                                end
                                else ' other '
                            end
                        ) not in (' other ') then ' AA & Card other '
                        when checkout_mart.IS_PBA_SLINGSHOT_CHECKOUT = 1 then case
                            when cf.plan_frequency = ' daily '
                            and cf.plan_interval = 30 then ' Shopify PiX '
                            when cf.plan_frequency = ' daily '
                            and cf.plan_interval = 15 then ' Shopify PiX '
                            when cf.plan_frequency = ' weekly '
                            and cf.loan_type = ' classic ' then ' Shopify PiX '
                            when cf.plan_frequency = ' weekly '
                            or cf.loan_type = ' affirm_go_v3 ' then ' Shopify PiX '
                            else ' Shopify IL '
                        end
                        when cf.plan_frequency = ' daily '
                        and cf.plan_interval = 30 then ' Core PiX '
                        when cf.plan_frequency = ' daily '
                        and cf.plan_interval = 15 then ' Core PiX '
                        when cf.plan_frequency = ' weekly '
                        and cf.loan_type = ' classic ' then ' Core PiX '
                        when cf.checkout_flow_type in (' affirm_go ', ' split_pay_go_v3 ')
                        or cf.loan_type = ' affirm_go_v3 '
                        or cf.plan_frequency = ' weekly ' then ' Core PiX '
                        else ' Core IL '
                    end
                )
                else ' not captured '
            end
        ) in (' Core IL ') then case
            when cf.apr > 0.09 then ' General Core IB '
            else ' General Core: 0 % / Fixed APR '
        end
        else case
            when nvl(cf.is_captured, 0) = 1 then (
                case
                    when (
                        case
                            when (
                                cf.guarantee_ari is not null
                                and aip.checkout_ari is not null
                            )
                            or (
                                cf.merchant_ari in (
                                    ' 4B1SGJYYDNJIXA3H ',
                                    ' ST8FQHFDIUWLVVQJ ',
                                    ' CH89JWTJBIWF8V1E ',
                                    ' WUSUUM3PA2YHQNHM '
                                )
                            ) then case
                                -- Affirm Card
                                when (
                                    (
                                        cm.product_platform = ' Anywhere '
                                        and chfc.checkout_issuing_instrument_type = ' debit_plus '
                                    )
                                    or cm.loan_product_group like ' Debit + % '
                                ) then (
                                    case
                                        when cf.plan_frequency = ' daily '
                                        and cf.plan_interval = 30 then ' Affirm Card PiX '
                                        when cf.plan_frequency = ' daily '
                                        and cf.plan_interval = 15 then ' Affirm Card PiX '
                                        when cf.plan_frequency = ' weekly '
                                        and cf.loan_type = ' classic ' then ' Affirm Card PiX '
                                        when cf.plan_frequency = ' weekly '
                                        or cf.loan_type = ' affirm_go_v3 ' then ' Affirm Card PiX '
                                        when cf.loan_type = ' classic '
                                        or cf.plan_frequency = ' monthly ' then ' Affirm Card IL '
                                        else ' Affirm Card Other '
                                    end
                                )
                                -- Affirm Anywhere
                                when cf.plan_frequency = ' daily '
                                and cf.plan_interval = 30 then ' AA PiX '
                                when cf.plan_frequency = ' daily '
                                and cf.plan_interval = 15 then ' AA PiX '
                                when cf.plan_frequency = ' weekly '
                                and cf.loan_type = ' classic ' then ' AA PiX '
                                when cf.plan_frequency = ' weekly '
                                or cf.loan_type = ' affirm_go_v3 ' then ' AA PiX '
                                when cf.loan_type = ' classic '
                                or cf.plan_frequency = ' monthly ' then ' AA IL '
                                else ' AA Other '
                            end
                            else ' other '
                        end
                    ) in (' AA IL ', ' Affirm Card IL ') then ' AA & Card IL '
                    when (
                        case
                            when (
                                cf.guarantee_ari is not null
                                and aip.checkout_ari is not null
                            )
                            or (
                                cf.merchant_ari in (
                                    ' 4B1SGJYYDNJIXA3H ',
                                    ' ST8FQHFDIUWLVVQJ ',
                                    ' CH89JWTJBIWF8V1E ',
                                    ' WUSUUM3PA2YHQNHM '
                                )
                            ) then case
                                -- Affirm Card
                                when (
                                    (
                                        cm.product_platform = ' Anywhere '
                                        and chfc.checkout_issuing_instrument_type = ' debit_plus '
                                    )
                                    or cm.loan_product_group like ' Debit + % '
                                ) then (
                                    case
                                        when cf.plan_frequency = ' daily '
                                        and cf.plan_interval = 30 then ' Affirm Card PiX '
                                        when cf.plan_frequency = ' daily '
                                        and cf.plan_interval = 15 then ' Affirm Card PiX '
                                        when cf.plan_frequency = ' weekly '
                                        and cf.loan_type = ' classic ' then ' Affirm Card PiX '
                                        when cf.plan_frequency = ' weekly '
                                        or cf.loan_type = ' affirm_go_v3 ' then ' Affirm Card PiX '
                                        when cf.loan_type = ' classic '
                                        or cf.plan_frequency = ' monthly ' then ' Affirm Card IL '
                                        else ' Affirm Card Other '
                                    end
                                )
                                -- Affirm Anywhere
                                when cf.plan_frequency = ' daily '
                                and cf.plan_interval = 30 then ' AA PiX '
                                when cf.plan_frequency = ' daily '
                                and cf.plan_interval = 15 then ' AA PiX '
                                when cf.plan_frequency = ' weekly '
                                and cf.loan_type = ' classic ' then ' AA PiX '
                                when cf.plan_frequency = ' weekly '
                                or cf.loan_type = ' affirm_go_v3 ' then ' AA PiX '
                                when cf.loan_type = ' classic '
                                or cf.plan_frequency = ' monthly ' then ' AA IL '
                                else ' AA Other '
                            end
                            else ' other '
                        end
                    ) in (' AA PiX ', ' Affirm Card PiX ') then ' AA & Card PiX '
                    when (
                        case
                            when (
                                cf.guarantee_ari is not null
                                and aip.checkout_ari is not null
                            )
                            or (
                                cf.merchant_ari in (
                                    ' 4B1SGJYYDNJIXA3H ',
                                    ' ST8FQHFDIUWLVVQJ ',
                                    ' CH89JWTJBIWF8V1E ',
                                    ' WUSUUM3PA2YHQNHM '
                                )
                            ) then case
                                -- Affirm Card
                                when (
                                    (
                                        cm.product_platform = ' Anywhere '
                                        and chfc.checkout_issuing_instrument_type = ' debit_plus '
                                    )
                                    or cm.loan_product_group like ' Debit + % '
                                ) then (
                                    case
                                        when cf.plan_frequency = ' daily '
                                        and cf.plan_interval = 30 then ' Affirm Card PiX '
                                        when cf.plan_frequency = ' daily '
                                        and cf.plan_interval = 15 then ' Affirm Card PiX '
                                        when cf.plan_frequency = ' weekly '
                                        and cf.loan_type = ' classic ' then ' Affirm Card PiX '
                                        when cf.plan_frequency = ' weekly '
                                        or cf.loan_type = ' affirm_go_v3 ' then ' Affirm Card PiX '
                                        when cf.loan_type = ' classic '
                                        or cf.plan_frequency = ' monthly ' then ' Affirm Card IL '
                                        else ' Affirm Card Other '
                                    end
                                )
                                -- Affirm Anywhere
                                when cf.plan_frequency = ' daily '
                                and cf.plan_interval = 30 then ' AA PiX '
                                when cf.plan_frequency = ' daily '
                                and cf.plan_interval = 15 then ' AA PiX '
                                when cf.plan_frequency = ' weekly '
                                and cf.loan_type = ' classic ' then ' AA PiX '
                                when cf.plan_frequency = ' weekly '
                                or cf.loan_type = ' affirm_go_v3 ' then ' AA PiX '
                                when cf.loan_type = ' classic '
                                or cf.plan_frequency = ' monthly ' then ' AA IL '
                                else ' AA Other '
                            end
                            else ' other '
                        end
                    ) not in (' other ') then ' AA & Card other '
                    when checkout_mart.IS_PBA_SLINGSHOT_CHECKOUT = 1 then case
                        when cf.plan_frequency = ' daily '
                        and cf.plan_interval = 30 then ' Shopify PiX '
                        when cf.plan_frequency = ' daily '
                        and cf.plan_interval = 15 then ' Shopify PiX '
                        when cf.plan_frequency = ' weekly '
                        and cf.loan_type = ' classic ' then ' Shopify PiX '
                        when cf.plan_frequency = ' weekly '
                        or cf.loan_type = ' affirm_go_v3 ' then ' Shopify PiX '
                        else ' Shopify IL '
                    end
                    when cf.plan_frequency = ' daily '
                    and cf.plan_interval = 30 then ' Core PiX '
                    when cf.plan_frequency = ' daily '
                    and cf.plan_interval = 15 then ' Core PiX '
                    when cf.plan_frequency = ' weekly '
                    and cf.loan_type = ' classic ' then ' Core PiX '
                    when cf.checkout_flow_type in (' affirm_go ', ' split_pay_go_v3 ')
                    or cf.loan_type = ' affirm_go_v3 '
                    or cf.plan_frequency = ' weekly ' then ' Core PiX '
                    else ' Core IL '
                end
            )
            else ' not captured '
        end
    end as captured_merchant_category,
    cf.loan_type as checkout_loan_type,
    cf.checkout_product_type,
    cf.checkout_flow_type,
    cf.offered_plan1_frequency as checkout_offered_plan_frequency1,
    cf.offered_plan2_frequency as checkout_offered_plan_frequency2,
    cf.offered_plan3_frequency as checkout_offered_plan_frequency3,
    cf.plan_frequency,
    cf.plan_interval,
    cf.plan_length,
    cf.term_length,
    cf.charge_schedule_amount,
    -- monthly_loan_index
    case
        when cf.plan_frequency = ' daily '
        and cf.plan_interval = 30 then 1
        when cf.plan_frequency = ' monthly ' then 1
        else 0
    end as monthly_loan_index,
    -- biweekly_loan_index
    1 - (
        case
            when cf.plan_frequency = ' daily '
            and cf.plan_interval = 30 then 1
            when cf.plan_frequency = ' monthly ' then 1
            else 0
        end
    ) as biweekly_loan_index,
    cf.total_amount as checkout_requested_amount,
    least(
        nvl(
            coft.offered_down_payment_money_amount1_chargedetails,
            999999
        ),
        nvl(
            coft.offered_down_payment_money_amount2_chargedetails,
            999999
        ),
        nvl(
            coft.offered_down_payment_money_amount3_chargedetails,
            999999
        )
    ) as checkout_min_dp_required_amount,
    cf.down_payment_money_amount as checkout_dp_amount,
    case
        when nvl(pq.is_applied, 0) > 0
        or cf.prequal_ari is not null then ' PQ '
        else ' Non PQ '
    end as prequal_flag,
    pq.approved_amount as prequal_approved_amount,
    nvl(cf.offered_apr1, pq.offered_apr_1) as offered_apr1,
    nvl(cf.offered_apr2, pq.offered_apr_2) as offered_apr2,
    nvl(cf.offered_apr3, pq.offered_apr_3) as offered_apr3,
    case
        when coalesce(
            cf.offered_apr1,
            cf.offered_apr2,
            cf.offered_apr3,
            pq.offered_apr_1,
            pq.offered_apr_2,
            pq.offered_apr_3
        ) is null then null
        else least(
            coalesce(cf.offered_apr1, pq.offered_apr_1, 9999),
            coalesce(cf.offered_apr2, pq.offered_apr_2, 9999),
            coalesce(cf.offered_apr3, pq.offered_apr_3, 9999)
        )
    end as min_offered_apr,
    case
        when coalesce(
            cf.offered_apr1,
            cf.offered_apr2,
            cf.offered_apr3,
            pq.offered_apr_1,
            pq.offered_apr_2,
            pq.offered_apr_3
        ) is null then null
        else greatest(
            coalesce(cf.offered_apr1, pq.offered_apr_1, -9999),
            coalesce(cf.offered_apr2, pq.offered_apr_2, -9999),
            coalesce(cf.offered_apr3, pq.offered_apr_3, -9999)
        )
    end as max_offered_apr,
    cf.apr as checkout_apr,
    case
        when cf.is_autopay_enabled = 1 then ' Autopay ON '
        else ' Autopay OFF '
    end as checkout_autopay_enabled_flag,
    coalesce(cf.fico_score, pq.fico_score, 0) as fico_score,
    case
        when nofile.charge_ari is not null then 1
        else 0
    end as nofile_index,
    -- funnel_itacs_version and funnel_itacs
    case
        when cf.loan_type = ' classic ' then nvl(cf.itacs_version_il, cf.itacs_version)
        when cf.loan_type in (
            ' split_pay_go_v3 ',
            ' affirm_go_v3 ',
            ' affirm_go '
        ) then nvl(cf.itacs_version_sp, cf.itacs_version)
        else nvl(cf.itacs_version, pq.itacs_version)
    end as pos_version,
    case
        when cf.loan_type = ' classic ' then COALESCE(cf.itacs_il_v1, cf.itacs_il, cf.itacs_v1, cf.itacs)
        when cf.loan_type in (
            ' split_pay_go_v3 ',
            ' affirm_go_v3 ',
            ' affirm_go '
        ) then COALESCE(cf.itacs_sp_v1, cf.itacs_sp, cf.itacs_v1, cf.itacs)
        else COALESCE(cf.itacs_v1, pq.itacs_v1, cf.itacs, pq.itacs)
    end as itacs_v1,
    nvl(cf.itacs, pq.itacs) as itacs,
    mdim.merchant_name,
    mdim.merchant_ari,
    mdim.merchant_industry,
    mdim.merchant_subindustry,
    mdim.merchant_platform,
    mdim.merchant_partner_ari,
    -- merchant_partner_definition
    case
        when mdim.merchant_partner_ari in (' I5KKNUB4GT63KC7T ') then ' AmazonPay '
        when mdim.merchant_partner_ari in (' S6ICJ0JGLE3VFQ7Z ') then ' ApplePay '
        when mdim.merchant_partner_ari in (' 0FF8PWHMP7UY52VF ') then ' GooglePay '
        else ' other '
    end as merchant_partner_flag,
    mdim.merchant_business_type,
    rev_seg.cs_division as merchant_revenue_segment,
    mdim.merchant_city,
    mdim.merchant_postal_code,
    mdim.merchant_state,
    case
        when mdim.merchant_created_dt is not null then datediff(
            month,
            nvl(mdim.merchant_created_dt::date, current_date),
            nvl(pq.start_date::date, cf.start_date::date)
        )
        else null
    end as merchant_tenure_months,
    mdim.merchant_product_type,
    hrm.high_risk_merchant_flag,
    hrm.high_risk_merchant_status,
    nvl(cbo.cbo_ind, 0) as confirmed_cbo_index,
    case
        when hrm.high_risk_merchant_flag like (' % CBO % ') then hrm.high_risk_merchant_flag
        else null
    end as cbo_hig_risk_merchant_flag,
    case
        when hrm.high_risk_merchant_flag like (' % CBO % ') then 1
        else 0
    end as cbo_hig_risk_merchant_index,
    cbo_msa.high_risk_msa_flag as cbo_high_risk_msa_flag,
    case
        when cbo_msa.high_risk_msa_flag is not null then 1
        else 0
    end as cbo_high_risk_msa_index,
    case
        when cbo_high_risk_msa_index = 1
        and cbo_hig_risk_merchant_index = 1 then 1
        else 0
    end as cbo_high_risk_merchant_and_msa_index,
    ua.region1_code as user_address_state_code,
    nvl(uw.msa, qm_msa_tbl.primary_cbsa_name) as user_address_msa,
    case
        when nvl(uw.student_loan_balance, 0) > 0 then 1
        else 0
    end as student_loan_index,
    uw.affirm_user_annual_income_dollars,
    uw.affirm_user_exposure_dollars,
    uw.affirm_loan_minimum_exposure_remaining_amount_dollars,
    case
        when uw.affirm_loan_minimum_exposure_remaining_amount_dollars is not null then nvl(uw.affirm_user_exposure_dollars, 0) + nvl(
            uw.affirm_loan_minimum_exposure_remaining_amount_dollars,
            0
        )
        else null
    end as affirm_user_exposure_limit_dollars
from
    { { source(' dbt_analytics ', ' checkout_funnel_v5 ') } } as cf full
    outer join (
        select
            application_ari,
            charge_ari,
            user_ari,
            prequal_ari,
            checkout_ari,
            start_date,
            decision_state,
            case
                when nvl(num_deciders_run, 0) > 0 then 1
                else 0
            end as is_applied,
            merchant_ari,
            declined_reason,
            approved_amount,
            offered_apr_1,
            offered_apr_2,
            offered_apr_3,
            fico_score,
            itacs_version,
            itacs_v1,
            itacs
        from
            { { source(' dbt_analytics ', ' prequal_funnel ') } }
        where
            nvl(num_deciders_run, 0) > 0
            and is_prequal_deduped = 1
            and decision_state in (' approved ', ' declined ')
    ) pq on cf.prequal_ari = pq.prequal_ari
    left join { { source(' users ', ' prequal ') } } upre on upre.ari = pq.prequal_ari
    /************** tables needed for AA and Card product definition at application level: ***********/
    left join { { source(' dbt_analytics ', ' anywhere_prequal_fact ') } } apf on cf.guarantee_ari = apf.guarantee_ari
    left join aa_prequal_card_status pq_card_label on apf.decision_id = pq_card_label.decision_id
    /************************************************************************************************/
    left join { { source(' dbt_analytics ', ' merchant_dim ') } } mdim on nvl(cf.merchant_ari, pq.merchant_ari) = mdim.merchant_ari
    left join { { source(
        ' creditcapabilities ',
        ' credit_high_risk_merchant_list '
    ) } } as hrm on mdim.merchant_ari = hrm.merchant_ari
    left join { { source(
        ' dbt_revenue ',
        ' merchant_revenue_segmentation_dim '
    ) } } as rev_seg on mdim.merchant_ari = rev_seg.merchant_ari
    left join { { source(' dbt_analytics ', ' checkout_session_fact ') } } as chfc on chfc.checkout_ari = cf.checkout_ari
    left join { { source(' dbt_analytics ', ' checkout_mart ') } } as checkout_mart on checkout_mart.checkout_ari = cf.checkout_ari
    left join { { source(' dbt_analytics ', ' charge_mart ') } } as cm on cm.charge_ari = cf.charge_ari
    left join { { source(
        ' dbt_analytics ',
        ' affirm_initiated_purchases_v3 '
    ) } } as aip on aip.checkout_ari = cf.checkout_ari
    left join { { source(' dbt_analytics ', ' user_dim ') } } ud ON nvl(pq.user_ari, cf.user_ari) = ud.user_ari
    left join { { source(' dbt_analytics ', ' person_stats_mart ') } } fpl on fpl.person_uuid = ud.person_uuid
    left join { { source(
        ' dbt_analytics ',
        ' checkout_offered_terms_fact_beta '
    ) } } as coft on coft.checkout_ari = cf.checkout_ari
    left join nofile on nofile.charge_ari = cf.charge_ari
    left join { { source(' creditanalytics ', ' cbo_loans_final_table ') } } cbo on cbo.charge_ari = cf.charge_ari
    left join uw_signals as uw on uw.application_ari = cf.application_ari
    left join { { source(' applications ', ' application ') } } as app on app.ari = cf.application_ari
    left join { { source(' users ', ' address ') } } as ua on app.billing_address_ari = ua.ari
    left join { { source(' quantitativemarkets ', ' zipmetrodata ') } } qm_msa_tbl on ua.postal_code = qm_msa_tbl.zip_code
    left join { { source(
        ' creditcapabilities ',
        ' cbo_high_risk_msa_list '
    ) } } as cbo_msa on cbo_msa.msa_name = nvl(uw.msa, qm_msa_tbl.primary_cbsa_name)
where
    (
        nvl(pq.start_date, cf.start_date)::date >= ' { { start_date } } '
        or nvl(cf.captured_dt, cf.first_capture_dt)::date >= ' { { start_date } } '
    )
    and coalesce(cf.is_checkout_applied, pq.is_applied, 0) > 0
