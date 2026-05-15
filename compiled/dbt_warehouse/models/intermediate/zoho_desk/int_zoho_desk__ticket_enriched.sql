

with tickets as (
    select * from `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__tickets`
),

ticket_details as (
    select * from `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__ticket_details`
),

ticket_metrics as (
    select * from `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__ticket_metrics`
),

contacts as (
    select * from `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__contacts`
),

accounts as (
    select * from `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__accounts`
),

departments as (
    select * from `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__departments`
),

agents as (
    select * from `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__agents`
),

enriched as (
    select
        -- ticket identity
        t.ticket_id,
        t.ticket_number,
        t.subject,
        t.web_url,

        -- status & classification
        t.status,
        t.status_type,
        t.priority,
        t.channel,
        t.category,
        t.sub_category,
        t.is_archived,
        t.is_spam,

        -- timestamps (raw)
        t.created_time,
        t.closed_time,
        t.due_date,
        t.response_due_date,

        -- account (with contact fallback when ticket has no direct account_id)
        coalesce(t.account_id, c.account_id) as account_id,
        coalesce(a_direct.account_name, a_via_contact.account_name) as account_name,

        -- contact
        t.contact_id,
        c.first_name as contact_first_name,
        c.last_name as contact_last_name,
        c.email as contact_email,

        -- assignee
        t.assignee_id,
        ag.name as assignee_name,

        -- department
        t.department_id,
        d.name as department_name,

        -- categories / nature (custom fields from ticket_details)
        td.cf_nature_des_demandes as nature_des_demandes,

        -- main categories
        td.cf_contrat_avenant,
        td.cf_creation_d_un_site,
        td.cf_demande_intervention,
        td.cf_inscription,
        td.cf_machines,
        td.cf_modification,
        td.cf_modification_d_un_site,
        td.cf_modifications,
        td.cf_rupture,
        td.cf_suivi_intervention,
        td.cf_s_systeme_de_paiement,
        td.cf_technique,

        -- sub-categories
        td.cf_s_equipements,
        td.cf_s_commande_directes,
        td.cf_s_commercial,
        td.cf_s_facturation,
        td.cf_s_gestion_des_cartes_privatives,
        td.cf_s_gestion_des_planogrammes,
        td.cf_s_recyclage,
        td.cf_s_remboursement,
        td.cf_s_remontees_personnel_neshu,
        td.cf_s_reappro,

        -- SLA flags
        td.is_over_due,
        td.is_response_overdue,
        td.is_escalated,
        td.resolution,

        -- conversation metrics
        t.thread_count,
        m.outgoing_count as agent_response_count,
        m.response_count,
        m.reopen_count,
        m.reassign_count,

        -- SLA durations (minutes)
        m.first_response_time_minutes,
        m.resolution_time_minutes,
        m.total_response_time_minutes

    from tickets as t
    left join ticket_details as td
        on t.ticket_id = td.ticket_id
    left join ticket_metrics as m
        on t.ticket_id = m.ticket_id
    left join contacts as c
        on t.contact_id = c.contact_id
    left join accounts as a_direct
        on t.account_id = a_direct.account_id
    left join accounts as a_via_contact
        on c.account_id = a_via_contact.account_id
    left join departments as d
        on t.department_id = d.department_id
    left join agents as ag
        on t.assignee_id = ag.agent_id
)

select * from enriched