from src import big_query as bq, utilities as utils, clean_business_name, record_linkage

def sf_accts(client, filter_by=None):

    query = """
            SELECT
              aoa.code AS AccountId,
              aoa.Account_Name AS AccountName,
              Outreach_Account_Natural_Name AS CleanName,
              aoa.Type AS AccountType,
              aoa.Website,
              aoa.Annual_Revenue AS AnnualRevenue,
              aoa.NumberofEmployees,
              aoa.SIC_Code AS SIC,
              aoa.IndustryProfileFinal AS IndustryProtfolio,
              aoa.IndustrySubPortfolio_GTMPlanningFinal AS IndustrySubPortfolio,
              aoa.Primary_Industry_c AS PrimaryIndustry,
              aoa.Sub_Industry AS SubIndustry,
              aoa.Billing_City AS BillingCity,
              aoa.BillingState,
              aoa.BillingPostalCode,
              aoa.BillingCountry,
              aoa.Final_pod AS POD,
              aoa.Final_Geo AS Geo,
              aoa.DNBDUNSNumber AS DUNSNumber,
              aoa.DnBCompanyRecord,
            FROM
              `ap-marketing-data-ops-prod.AoA_MarketingOps.Account` AS aoa
          """

    if filter_by is not None:
       query = query + " WHERE " + filter_by

    df_sf_accts = bq.execute_query(client, query)

    # Cleanse Website and Domain
    df_sf_accts['WebsiteClean'], df_sf_accts['DomainClean'] = zip(*df_sf_accts['Website'].apply(utils.clean_website_domain))

    clean_name = clean_business_name.CleanBusinessName(client)
    df_sf_accts['AccountNameClean'] = clean_name.clean_names(df_sf_accts['AccountName'])


    df_sf_accts = df_sf_accts.add_prefix("SF_")
    df_sf_accts['SF_Index']= df_sf_accts['SF_AccountId']
    df_sf_accts = df_sf_accts.set_index("SF_Index")
    df_sf_accts.sort_index(inplace=True)

    df_sf_accts = utils.convert_column_types(df_sf_accts, str_cols=[], int_cols=['SF_SIC'], float_cols=['SF_AnnualRevenue', 'SF_NumberofEmployees'], category_cols=[], datetime_cols=[])


    return df_sf_accts


def sf_opps(client, filter_by=None):
    query = """
            SELECT
              AccountId,
              OpportunityId,
              Display_Name AS OpportunityName,
              New_Or_Expand,
              SF_ACV_New_Expand_Converted AS ACVNewExpandConverted,
              StageName,
              True_Stage,
              CAST(CreatedDate AS DATETIME) AS CreatedDate,
              CAST(First_Opp_Created_Date_c AS DATETIME) AS FirstOppCreatedDate,
              DATE_SUB(CAST(CreatedDate AS DATETIME), INTERVAL 13 MONTH) AS AdustedCreatedDate,
              CAST(Stage1_Date AS DATETIME) AS Stage1Date,
              CAST(Stage2_Date AS DATETIME) AS Stage2Date,
              CAST(Stage3_Date AS DATETIME) AS Stage3Date,
              CAST(Stage4_Date AS DATETIME) AS Stage4Date,
              CAST(Final_CloseDate AS DATETIME) AS CloseDate,
              SQL_Source_Type_c AS SQLSourceType,
              SQL_SourcefromSalesFcst AS SQLSourcefromSalesFcst,
              CAST(SQL_Created_Date_c AS DATETIME) AS SQLCreatedDate,
              CAST(First_MQL_Created_Date_c AS DATETIME) AS FirstMQLCreatedDate,
              CAST(First_Approved_SDR_Meeting_Date__c AS DATETIME) AS FirstApprovedSDRMeetingDate,
              Record_Type_text_c AS RecordType,
              Reason__c AS Reason,
              Reason_Details__c AS ReasonDetails,
              Duplicate_Opportunity_Link__c AS DuplicateOpportunityLink
            FROM
              `ap-marketing-data-ops-prod.AoA_MarketingOps.Opportunity`
            """

    if filter_by is not None:
        query = query + " WHERE " + filter_by 

    df_sf_opps = bq.execute_query(client, query)

    df_sf_opps = utils.convert_column_types(
                    df_sf_opps, str_cols=[], 
                    int_cols=['ACVNewExpandConverted'], 
                    float_cols=[], 
                    category_cols=['New_Or_Expand', 'StageName', 'True_Stage', 'SQLSourceType', 'SQLSourcefromSalesFcst', 'RecordType', 'Reason'], 
                    datetime_cols=[])


    df_sf_opps['Index']= df_sf_opps['OpportunityId']
    df_sf_opps = df_sf_opps.set_index('Index')
    df_sf_opps.sort_index(inplace=True)
    df_sf_opps = df_sf_opps.add_prefix('OPP_')

    return df_sf_opps

def sf_campaigns(client, filter_by=None):

    query = """
              SELECT
                Code AS CampaignId,
                Name AS CampaignName,
                Campaign_ParentID AS CampaignParentId,
                Campaign_ParentName AS CampaignParentName,
                Final_Channel AS CampaignChannel,
                LOB AS CampaignLOB,
                Industry AS CampaignIndustry,
                IsActive AS CampaignStatus,
              FROM
                `ap-marketing-data-ops-prod.AoA_MarketingOps.Campaign` as cm
            """

    if filter_by is not None:
        query = query + " WHERE " + filter_by 

    df_sf_campaigns = bq.execute_query(client, query)

    df_sf_campaigns = utils.convert_column_types(
        df_sf_campaigns, str_cols=[], 
        int_cols=[], 
        float_cols=[], 
        category_cols=['CampaignChannel', 'CampaignChannels', 'CampaignLOB', 'CampaignIndustry', 'CampaignStatus'], 
        datetime_cols=[])

    df_sf_campaigns['Index']= df_sf_campaigns['CampaignId']
    df_sf_campaigns = df_sf_campaigns.set_index('Index')
    df_sf_campaigns.sort_index(inplace=True)
    df_sf_campaigns = df_sf_campaigns.add_prefix('CMP_')

    return df_sf_campaigns

def sf_campaign_members(client, filter_by=None):

    query = """
        SELECT
          cm.Code AS MemberId,
          cm.ContactLeadID AS ContactId,
          cm.Type AS ContactType,
          cm.LOB,
          cm.Final_AccountID AS AccountId,
          cm.Account_Name AS AccountName,
          cm.CampaignId,
          cm.Campaign,
          cm.CampaignMember_Status AS CampaignMemberStatus,
          cm.HasResponded,
          CAST(cm.CreatedDate AS DATETIME) AS CreatedDate,
          CAST(cm.FirstRespondedDate AS DATETIME) AS FirstRespondedDate,
          cm.Channel_Campaign AS ChannelCampaign,
          cm.Channel_Medium AS ChannelMedium,
          cm.Channel_Source AS ChannelSource,
          cm.Channel_Campaign_Final AS ChannelCampaignFinal,
          cm.Final_Campaign_Channel AS FinalCampaignChannel,
          cm.edw_lead_channel AS EDWChannel,
          cm.Campaign_Channels AS CampaignChannels,
          cnct.Name AS ContactName,
          cnct.Email,
          cnct.Job_Level AS JobLevel,
          cnct.Job_Function AS JobFunction,
        FROM
          `ap-marketing-data-ops-prod.AoA_MarketingOps.CampaignMember` AS cm
        LEFT JOIN
          `ap-marketing-data-ops-prod.AoA_MarketingOps.ContactLead` AS cnct
        ON
          cnct.Code = cm.ContactLeadID
        """
    if filter_by is not None:
        query = query + " WHERE " + filter_by 

    df_sf_campaign_members = bq.execute_query(client, query)

    df_sf_campaign_members = utils.convert_column_types(
                                  df_sf_campaign_members, str_cols=[], 
                                  int_cols=['CampaignMemberStatus', 'HasResponded'], 
                                  float_cols=[], 
                                  category_cols=['ContactType', 'LOB', 'ChannelCampaign', 'ChannelMedium', 'ChannelSource', 'ChannelCampaignFinal',
                                    'FinalCampaignChannel', 'EDWChannel', 'CampaignChannels', 'JobLevel', 'JobFunction'], 
                                  datetime_cols=[])

    df_sf_campaign_members['Index']= df_sf_campaign_members['MemberId']
    df_sf_campaign_members = df_sf_campaign_members.set_index('Index')
    df_sf_campaign_members.sort_index(inplace=True)
    df_sf_campaign_members = df_sf_campaign_members.add_prefix('CM_')

    return df_sf_campaign_members
