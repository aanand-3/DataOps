import re
from cleancorp import CleanCorp
from src import big_query

class CleanBusinessName:
    def __init__(self, bigquery_client):
        """Initialize with a BigQuery client."""
        self.client = bigquery_client
        self._stopwords = None
        self._countries = None
        self._stopwords_pattern = None
        self._country_pattern = None

    def _fetch_stopwords(self):
        """Fetch stopwords from a BigQuery table and cache them."""
        if self._stopwords is None:
            query = """
            SELECT string_field_0 FROM `ap-marketing-data-ops-prod.MaintenanceDB.Stopwords`
            """
            results = big_query.execute_query(self.client, query)
        return [row for row in results.string_field_0]

    def _fetch_countries(self):
        """Fetch country names from a BigQuery table, escape them, and cache."""
        if self._countries is None:
            query = """
            SELECT string_field_0 FROM `ap-marketing-data-ops-prod.MaintenanceDB.Countries`
            """
            results = big_query.execute_query(self.client, query)
        return [re.escape(name) for name in results.string_field_0]

    def _compile_pattern(self, pattern_type):
        """Compile and cache regex patterns for stopwords and countries."""
        if pattern_type == 'stopwords' and self._stopwords_pattern is None:
            self._stopwords_pattern = re.compile(r'\b(?:' + '|'.join(self._fetch_stopwords()) + r')\b', flags=re.IGNORECASE)
        elif pattern_type == 'country' and self._country_pattern is None:
            self._country_pattern = re.compile(r'\b(?:' + '|'.join(self._fetch_countries()) + r')\b', flags=re.IGNORECASE)
        return getattr(self, f"_{pattern_type}_pattern")

    def clean_business_name(self, name):
        """Clean the business name using a hypothetical external cleaning library."""
        clean = CleanCorp(name)  # Assuming CleanCorp is implemented elsewhere
        return clean.clean_name.title()

    def apply_regex_patterns(self, text):
        """Apply generic regex transformations to clean text."""
        patterns = [r"\([^()]*\)|\[[^[\]]*\]|\([^()]*$|\[[^[\]]*$", r"[,-]", r"\s+"]

        for pattern in patterns:
            text = re.sub(pattern, " ", text, flags=re.MULTILINE)
        return text.strip()

    def clean_names(self, names):
        """Apply all cleaning steps to a pandas Series of names."""
        def clean_text(text):
          if text is not None:
            text = self.clean_business_name(text)
            text = self.apply_regex_patterns(text)
            text = re.sub(self._compile_pattern('stopwords'), '', text).strip()
            text = re.sub(self._compile_pattern('country'), '', text).strip()
          return text

        return names.apply(clean_text)