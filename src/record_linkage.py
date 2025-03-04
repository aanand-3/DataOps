import re
import pandas as pd
import recordlinkage as rl
from src import utilities as utils

class RecordLinkage:
    """
    A class for matching records in two dataframes using the RecordLinkage library.

    Args:
    df1 (pandas.DataFrame): The first dataframe to match.
    df2 (pandas.DataFrame): The second dataframe to match.
    match_type (str): Defines the matching criteria.

    Example:
    matcher = RecordLinkage(df1, df2, 'name')
    df_matches = matcher.get_potential_matches()
    """

    def __init__(self, df1, df2, match_type, confidence_score=False, secondary=False):
        self.df1 = df1
        self.df2 = df2
        self.confidence_score = confidence_score
        self.secondary = secondary
        self._define_matching_rules(match_type)
        self._find_missing_cols()

    def _create_indexer(self):
        """
        Create a recordlinkage indexer object based on the matching rules.

        Returns:
        recordlinkage.Index: A recordlinkage indexer object.
        """
        indexer = rl.Index()
        index_rules = self.matching_rules.get('index', {})

        # Process 'block' rules
        block_rules = index_rules.get("block", {})
        for left_on, right_on in block_rules.items():
          for col in right_on:
            indexer.block(left_on=left_on, right_on=col)

        # Process 'sortedneighbour' rules
        sortedneighbour_rules = index_rules.get("sortedneighbour", {})
        for left_on, right_on in sortedneighbour_rules.items():
          for col in right_on:
            indexer.sortedneighbourhood(left_on=left_on, right_on=col)

        print(f"Blocks: {indexer.algorithms}.")
        return indexer

    def _create_comparator(self, n_jobs=4):
        """
        Create a recordlinkage comparator object based on the matching rules.

        Args:
        n_jobs (int, optional): The number of parallel jobs to run. Default is 4.

        Returns:
        recordlinkage.Compare: A recordlinkage comparator object.
        """
        comparison = rl.Compare(n_jobs=n_jobs)
        compare_rules = self.matching_rules.get('compare', {})

        for left_on, right_on in compare_rules.items():
            label = left_on.replace("SF_", "Match_")
            for col in right_on:
                if self.df1[left_on].dtype == self.df2[col].dtype:
                    if self.df1[left_on].dtype == 'object':
                        method = "cosine" #"jarowinkler"
                        comparison.string(left_on, col, method=method, threshold=0.85, label=f"{label}_{col}")
                    else:
                        comparison.exact(left_on, col, label=f"{label}_{col}")
                else:
                    print(f"Warning: Column '{left_on}' in df1 and column '{col}' in df2 have different data types.")

        print(f"Comparison Features: {comparison.features}.")
        return comparison

    def get_potential_matches(self):
        """
        Identify potential matches between two dataframes.

        Returns:
        pandas.DataFrame: A dataframe of potential matches.
        """
        indexer = self._create_indexer()
        comparator = self._create_comparator()
        candidate_links = indexer.index(self.df1, self.df2)

        print(f"Potential Duplicates Found: {len(candidate_links)}.")

        self.features = comparator.compute(candidate_links, self.df1, self.df2)
        self.features = self.features[self.features.sum(axis=1) >= 1].reset_index()
        self.features["Match_zScore"] = self.features.filter(like="Match_").sum(axis=1)

        if self.confidence_score:
            self._match_confidence()

        # Format the output dataframe
        self._format_output()
        return self.features

    def _match_confidence(self):

        # Weights for each attribute
        weights = {
            'Match_AccountName|Match_WebsiteClean|Match_Domain': 40,
            'Match_BillingCity': 20,
            'Match_BillingStateClean': 10,
            'Match_BillingPostalCode': 20,
            'Match_BillingCountryClean': 10
        }

        self.features['Match_zAccount'] = self.features.filter(regex='Match_AccountName|Match_WebsiteClean|Match_Domain').sum(axis=1)

        # Initialize the Match_Score column
        self.features['Match_zConfidence'] = 0

        # Calculate the weighted score for each attribute and add to the Match_Score
        for key, weight in weights.items():
            # Use .filter(like=key) to select the column dynamically
            df_filtered = self.features.filter(regex=key)
            score_component = df_filtered.sum(axis=1).apply(lambda x: weight if x > 0 else 0)
            self.features['Match_zConfidence'] += score_component

        # Normalize the scores to a percentage (0-100 scale)
        max_score = sum(weights.values())
        self.features['Match_zConfidence'] = (self.features['Match_zConfidence'] / max_score * 100).round().astype(int).fillna(0)
        self.features = utils.calculate_bins(self.features, column_name=['Match_zConfidence'], bin_edges=[-1, 0, 50, 60, 70, 80])
        
        return self.features

    def _format_output(self):
        """
        Format the output dataframe.

        Args:
        df_matches (pandas.DataFrame): Dataframe containing potential matches.
        """

        if self.secondary:
          self.features = pd.merge(self.df2, self.features, on=self.df2.index.name, how='left')
          self.features = pd.merge(self.features, self.df1, on=self.df1.index.name, how='left', suffixes=('_df2', '_df1'))
        else:
          self.features = pd.merge(self.df1, self.features, on=self.df1.index.name, how='left')
          self.features = pd.merge(self.features, self.df2, on=self.df2.index.name, how='left', suffixes=('_df1', '_df2'))

        match_sf_cols = [col for col in self.features.columns if col.startswith('Match_')]
        out_cols = [self.df1.index.name] + list(self.df1.columns) + sorted(match_sf_cols) + [self.df2.index.name] + list(self.df2.columns)
        # self.features = utils.fillna_custom(self.features[out_cols])

        # Convert these columns to integers
        for col in [col for col in self.features.filter(like="Match_").columns if '_bins' not in col]:
            self.features[col] = pd.to_numeric(self.features[col], errors='coerce').fillna(0).astype(int)

        self.features.reset_index(drop=True, inplace=True)

        return self.features



    def _find_missing_cols(self):
        """
        Find missing labels in two sets of columns based on a mapping of column names.

        Returns:
        tuple: Two lists of missing labels in df1 and df2, respectively.
        """
        df1_cols, df2_cols = self._extract_df_columns()
        df1_missing_cols = [col for col in df1_cols if col not in self.df1.columns]
        df2_missing_cols = [col for col in df2_cols if col not in self.df2.columns]

        print(f"Missing labels in df1: {df1_missing_cols}")
        print(f"Missing labels in df2: {df2_missing_cols}")

        return df1_missing_cols, df2_missing_cols

    def _extract_df_columns(self):
        """
        Extract leaf keys and values from the matching rules.

        Returns:
        tuple: Two lists containing leaf keys and values.
        """
        df1_cols = []
        df2_cols = []

        def recurse(data_dict):
            for key, value in data_dict.items():
                # Use recursion to handle nested dictionaries
                if isinstance(value, dict):
                    recurse(value)
                else:
                    # Process lists or scalar values directly
                    if isinstance(value, list):
                        filtered_values = [v for v in value if v is not None]
                        if filtered_values:  # Only add key if there are non-None values
                            df1_cols.extend([key] * len(filtered_values))
                            df2_cols.extend(filtered_values)
                    elif value is not None:  # Handle scalars directly
                        df1_cols.append(key)
                        df2_cols.append(value)
                        

        recurse(self.matching_rules)
        return df1_cols, df2_cols

    def _define_matching_rules(self, match_type):
        """
        Define matching rules based on the match type.

        Returns:
        dict: Matching rules dictionary.
        """

        if isinstance(match_type, dict):
            self.matching_rules = match_type
            return

        if match_type == "ZoomInfo":
            self.matching_rules = {
                "index": {"block": {"SF_AccountId": ["ZI_AccountId"]},
                          "sortedneighbour": {}},
                "compare": {
                    "SF_AccountName": ["ZI_AccountName"],
                    "SF_AccountNameClean": ["ZI_AccountNameClean"],
                    "SF_WebsiteClean": ["ZI_WebsiteClean"],
                    "SF_DomainClean": ["ZI_DomainClean"],
                    "SF_BillingCity": ["ZI_City"],
                    "SF_BillingStateClean": ["ZI_StateClean"],
                    "SF_BillingPostalCode": ["ZI_PostalCode"],
                    "SF_BillingCountryClean": ["ZI_CountryClean"],
                    "SF_AnnualRevenue": ["ZI_AnnualRevenue"],
                    "SF_NumberofEmployees": ["ZI_NumberofEmployees"],
                    "SF_SIC": ["ZI_SICCode"],
                },
            }
            return
        if match_type == "DNB":
            self.matching_rules = {
                "index": {"block": {"SF_DNBCompanyProfile": ["DNB_Id"]},
                          "sortedneighbour": {}},
                "compare": {
                    "SF_AccountName": ["DNB_BusinessName"],
                    "SF_AccountNameClean": ["DNB_BusinessNameClean"],
                    "SF_WebsiteClean": ["DNB_WebsiteClean"],
                    "SF_DomainClean": ["DNB_DomainClean"],
                    "SF_BillingCity": ["DNB_City"],
                    "SF_BillingStateClean": ["DNB_StateClean"],
                    "SF_BillingPostalCode": ["DNB_PostalCode"],
                    "SF_BillingCountryClean": ["DNB_CountryClean"],
                    "SF_DUNSNumber": ["DNB_DUNSNumber"],
                    "SF_AnnualRevenue": ["DNB_AnnualRevenue"],
                    "SF_NumberofEmployees": ["DNB_NumberofEmployees"],
                    "SF_SIC": ["DNB_SICCode"],
                },
            }
            return
        if match_type == "name":
            self.matching_rules = {
                "index": {"block": {},
                          "sortedneighbour": {"SF_AccountName": ["INP_Company", "INP_Outreach_Account_Natural_Name"],
                                              "SF_CleanName": ["INP_Outreach_Account_Natural_Name"]
                                              }},
                "compare": {
                    "SF_AccountName": ['INP_Company', 'INP_Outreach_Account_Natural_Name'],
                    "SF_CleanName": ['INP_Outreach_Account_Natural_Name'],
                },
            }
            return
        
        raise ValueError(f"Unknown match_type: {match_type}")