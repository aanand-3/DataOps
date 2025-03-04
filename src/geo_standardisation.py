from unidecode import unidecode
from src import utilities as utils


class GeoStandardisation:
  def __init__(self, client):
    self.client = client
    self._fetch_country()
    self._fetch_state()
  
  def _fetch_country(self):
      query = """
                  SELECT
                    id AS Id,
                    name AS Name,
                    iso3 AS ISO3,
                    iso2 AS ISO2,
                    capital AS Capital,
                    latitude AS Latitude,
                    longitude AS Longitude,
                    tld AS TLD,
                    native AS Native,
                    nationality AS Nationality
                  FROM
                    `ap-marketing-data-ops-prod.MaintenanceDB.Geo_Country`
              """
      self.df_country = utils.execute_query(self.client, query)
      self.df_country = fix_country(self.df_country)

  def _fetch_state(self):
      query = """
        SELECT
          id AS Id,
          name AS Name,
          country_id AS CountryId,
          country_code AS CountryISO2,
          country_name AS CountryName,
          state_code AS Code,
          type AS Type,
          latitude AS Latitude,
          longitude AS Longitude
        FROM
          `ap-marketing-data-ops-prod.MaintenanceDB.Geo_States`
      """

      self.df_state = utils.execute_query(self.client, query)
      self.df_state = fix_states(self.df_state)
  
  def _fetch_city(self):
      pass

  def _standardize_country(self, df, col):
    # Create a set of valid country names
    valid_country = set(self.df_country['CountryName'])
    self.df_country['Name_Eng'] = self.df_country['Name'].apply(unidecode)

    # Create a dictionary to map country names and codes to standardized values
    country_map = {}
    for col_name in ['CountryName', 'Name', 'Name_Eng', 'ISO3', 'ISO2']:
        country_map.update(dict(zip(self.df_country[col_name].str.lower().str.strip(), self.df_country['CountryName'])))

    # Apply the mapping to the country column
    df[f"{col}Clean"] = df[col].str.lower().str.strip().map(country_map).fillna(df[col].str.strip())

    # Check if the standardized country is valid
    df[f"{col}Valid"] = df[col].isin(valid_country).astype(int)
    df[f"{col}CleanValid"] = df[f"{col}Clean"].isin(valid_country).astype(int)

    return df

  def _standardize_states(self, df, col):

      # Create a set of valid state names
      valid_state = set(self.df_state['StateName'])
      self.df_state['Name_Eng'] = self.df_state['Name'].apply(unidecode)

      # Create a dictionary to map state names and codes to standardized values
      state_map = {}
      for col_name in ['StateName', 'Name_Eng', 'Name', 'Code']:
          state_map.update(dict(zip(self.df_state[col_name].str.lower().str.strip(), self.df_state['StateName'])))

      # Apply the mapping to the state column
      df[f"{col}Clean"] = df[col].str.lower().str.strip().map(state_map).fillna(df[col].str.strip())

      # Check if the standardized state is valid
      df[f"{col}Valid"] = df[col].isin(valid_state).astype(int)
      df[f"{col}CleanValid"] = df[f"{col}Clean"].isin(valid_state).astype(int)

      return df


def fix_country(df):
    country_map = {
        'The Bahamas': 'Bahamas',
        'Virgin Islands (British)': 'British Virgin Islands',
        'Cape Verde': 'Cabo Verde',
        'Democratic Republic of the Congo': 'Congo, Democratic Republic of the',
        'Czech Republic': 'Czechia',
        'Fiji Islands': 'Fiji',
        'Aland Islands': 'Åland Islands',
        'Gambia The': 'Gambia',
        'Palestinian Territory Occupied': 'Palestine',
        'Vatican City State (Holy See)': 'Holy See (Vatican City State)',
        'Hong Kong S.A.R.': 'Hong Kong',
        'Cote D\'Ivoire (Ivory Coast)': 'Côte d\'Ivoire',
        'North Korea': 'Korea, Democratic People\'s Republic of',
        'South Korea': 'Korea, Republic of',
        'Macau S.A.R.': 'Macao',
        'Curaçao': 'Curaçao',
        'Sint Maarten (Dutch part)': 'Sint Maarten',
        'Bonaire, Sint Eustatius and Saba': 'Bonaire, Sint Eustatius, and Saba',
        'Micronesia': 'Micronesia, Federated States of',
        'Pitcairn Island': 'Pitcairn Islands',
        'East Timor': 'Timor-Leste',
        'Russia': 'Russian Federation',
        'Saint-Barthelemy': 'Saint Barthelemy',
        'Saint Helena': 'Saint Helena, Ascension and Tristan da Cunha',
        'Saint-Martin (French part)': 'Saint Martin',
        'Svalbard And Jan Mayen Islands': 'Svalbard and Jan Mayen',
        'Swaziland': 'Eswatini',
        'Guernsey and Alderney': 'Guernsey',
        'Man (Isle of)': 'Isle of Man',
        'United States': 'United States of America',
        'Virgin Islands (US)': 'U. S. Virgin Islands',
        'Wallis And Futuna Islands': 'Wallis and Futuna'
    }

    df['CountryName'] = df['Name'].replace(country_map)

    return df

def fix_states(df):

    # ls = ['District', 'Municipality', 'Province', 'Region', 'County', 'Prefecture', 'Department', 'Oblast', 'Atoll', 'City of', 'London Borough of ']
    # df['State_Name'] = df['State_Name'].replace(ls, '', regex=True)

    state_map = {
        'Central Singapore': 'Singapore',
        'Metro Manila': 'Manila',
        'Auckland Region': 'Auckland',
        'North Rhine-Westphalia': 'Nordrhein-Westfalen',
        'Hong Kong SAR': 'Hong Kong',
        'Lower Saxony': 'Niedersachsen',
        'Ile de France': 'Ile-de-France',
        'North Holland': 'Noord-Holland',
        'South Holland': 'Zuid-Holland',
        'Hesse': 'Hessen',
        'Bavaria': 'Bayern'
    }
    df['StateName'] = df['Name'].replace(state_map)
    return df
