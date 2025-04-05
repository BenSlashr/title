# SEO Title Analyzer

This application analyzes SEO data from Google Search Console and provides title tag optimization suggestions.

## Features

- Reads CSV files exported from Google Search Console
- Extracts title tags from URLs via web scraping
- Compares keywords that each URL ranks for with words in the title tag
- Generates a report identifying:
  - URLs whose title doesn't contain the main keywords
  - Title length (with alerts for titles > 60 characters)
  - A simple optimization score for each URL
  - Suggested improved title for each URL

## Setup

1. Install dependencies:
```
pip install -r requirements.txt
```

2. Run the application:
```
uvicorn app.main:app --reload
```

3. Open your browser and navigate to `http://localhost:8000`

## Usage

1. Export your data from Google Search Console as a CSV file
2. Upload the CSV file through the web interface
3. View the analysis results and optimization suggestions
