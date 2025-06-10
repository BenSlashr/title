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

### Développement local

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the application:
```bash
uvicorn app.main:app --reload
```

3. Open your browser and navigate to `http://localhost:8000`

### Déploiement avec Docker

#### Déploiement simple (racine du domaine)

```bash
# Construire et lancer l'application
docker-compose up -d
```

#### Déploiement sur un sous-répertoire (exemple.com/nom-outil/)

1. Copier le fichier de configuration :
```bash
cp env.example .env
```

2. Modifier le fichier `.env` :
```bash
ROOT_PATH=/nom-outil
GEMINI_API_KEY=your_api_key_here
```

3. Construire et lancer :
```bash
docker-compose up -d
```

#### Avec Nginx (reverse proxy)

1. Copier la configuration Nginx :
```bash
cp nginx.conf.example nginx.conf
```

2. Modifier le fichier selon votre domaine et sous-répertoire

3. Lancer avec Nginx :
```bash
docker-compose --profile with-nginx up -d
```

### Variables d'environnement

- `ROOT_PATH` : Sous-répertoire de déploiement (ex: `/nom-outil` pour exemple.com/nom-outil/)
- `PORT` : Port d'écoute (défaut: 8000)
- `GEMINI_API_KEY` : Clé API Gemini pour les suggestions IA

### Configuration Nginx

Pour servir l'application sur un sous-répertoire, votre configuration Nginx doit inclure :

```nginx
location /nom-outil/ {
    proxy_pass http://localhost:8000/;
    proxy_set_header X-Forwarded-Prefix /nom-outil;
    # ... autres headers
}
```

## Usage

1. Export your data from Google Search Console as a CSV file
2. Upload the CSV file through the web interface
3. View the analysis results and optimization suggestions
