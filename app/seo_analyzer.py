import os
import re
import csv
import logging
import requests
import pandas as pd
from typing import List, Tuple, Dict, Set, Optional, Any
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import asyncio
import openai
from openai import AsyncOpenAI
import requests
from bs4 import BeautifulSoup
import re
from typing import Dict, List, Set, Tuple, Any
import time
import random
from collections import defaultdict
from urllib.parse import urlparse
import logging
import asyncio
import aiohttp
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import unicodedata
import numpy as np
from sentence_transformers import SentenceTransformer, util
from sklearn.cluster import AgglomerativeClustering
import nest_asyncio
import google.generativeai as genai

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Maximum number of concurrent requests
MAX_WORKERS = 20
# Timeout for HTTP requests (seconds)
REQUEST_TIMEOUT = 5

async def scrape_title_async(url: str, session: aiohttp.ClientSession) -> Tuple[str, str, str]:
    """
    Scrape the title tag and H1 from a given URL asynchronously.
    
    Args:
        url: The URL to scrape
        session: aiohttp ClientSession to use for requests
        
    Returns:
        Tuple of (url, title, h1)
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Add timeout to avoid hanging on slow servers
        async with session.get(url, headers=headers, timeout=REQUEST_TIMEOUT) as response:
            if response.status != 200:
                logger.warning(f"Error scraping {url}: HTTP status {response.status}")
                return url, "", ""
                
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            
            # Extract title
            title_tag = soup.find('title')
            title = title_tag.text.strip() if title_tag and title_tag.text else ""
            
            # Extract H1
            h1_tag = soup.find('h1')
            h1 = h1_tag.text.strip() if h1_tag and h1_tag.text else ""
            
            return url, title, h1
    except Exception as e:
        logger.warning(f"Error scraping {url}: {str(e)}")
        return url, "", ""

def scrape_title(url: str) -> str:
    """
    Scrape the title tag from a given URL (synchronous version for backward compatibility).
    
    Args:
        url: The URL to scrape
        
    Returns:
        The title tag text or an empty string if not found
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        title_tag = soup.find('title')
        
        if title_tag and title_tag.text:
            return title_tag.text.strip()
        return ""
    except Exception as e:
        logger.warning(f"Error scraping {url}: {str(e)}")
        return ""

def scrape_title_with_retry(url: str, max_retries: int = 2) -> str:
    """
    Scrape title with retry logic for resilience.
    
    Args:
        url: URL to scrape
        max_retries: Maximum number of retry attempts
        
    Returns:
        Title or empty string if not found
    """
    retries = 0
    while retries <= max_retries:
        try:
            # Increase timeout for each retry
            timeout = REQUEST_TIMEOUT * (retries + 1)
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            # Add a small random delay to avoid being blocked
            if retries > 0:
                delay = random.uniform(1.0, 3.0)
                time.sleep(delay)
                
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            title_tag = soup.find('title')
            
            if title_tag and title_tag.text:
                return title_tag.text.strip()
            return ""
            
        except requests.exceptions.Timeout:
            retries += 1
            if retries <= max_retries:
                logger.info(f"Timeout for {url}, retrying ({retries}/{max_retries})...")
            else:
                logger.warning(f"Max retries reached for {url}")
                return ""
                
        except requests.exceptions.HTTPError as e:
            # Don't retry for client errors (4xx)
            if 400 <= e.response.status_code < 500:
                logger.warning(f"Client error {e.response.status_code} for {url}")
                return ""
            
            # Retry for server errors (5xx)
            retries += 1
            if retries <= max_retries:
                logger.info(f"Server error {e.response.status_code} for {url}, retrying ({retries}/{max_retries})...")
            else:
                logger.warning(f"Max retries reached for {url}")
                return ""
                
        except Exception as e:
            retries += 1
            if retries <= max_retries:
                logger.info(f"Error for {url}: {str(e)}, retrying ({retries}/{max_retries})...")
            else:
                logger.warning(f"Max retries reached for {url}")
                return ""
    
    return ""

def scrape_titles_threaded(urls: List[str], max_workers: int = MAX_WORKERS) -> Dict[str, str]:
    """
    Scrape titles using multi-threading for parallel processing.
    
    Args:
        urls: List of URLs to scrape
        max_workers: Maximum number of threads to use
        
    Returns:
        Dictionary mapping URLs to titles
    """
    results = {}
    total_urls = len(urls)
    
    logger.info(f"Starting threaded scraping of {total_urls} URLs with {max_workers} workers")
    
    # Group URLs by domain to avoid hammering a single domain
    domain_urls = group_urls_by_domain(urls)
    
    # Track problematic domains
    problematic_domains = set()
    
    # Flatten the grouped URLs while respecting domain distribution
    distributed_urls = []
    remaining = True
    
    while remaining:
        remaining = False
        for domain, domain_url_list in list(domain_urls.items()):
            if domain_url_list:
                distributed_urls.append(domain_url_list.pop(0))
                remaining = True
    
    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_url = {executor.submit(scrape_title_with_retry, url): url for url in distributed_urls}
        
        # Process results as they complete
        completed = 0
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                title = future.result()
                results[url] = title
                completed += 1
                
                # Log progress every 10 URLs or at the end
                if completed % 10 == 0 or completed == total_urls:
                    logger.info(f"Scraped {completed}/{total_urls} URLs ({(completed/total_urls)*100:.1f}%)")
                
                # If we got a title, the domain is not problematic
                domain = urlparse(url).netloc
                if domain in problematic_domains and title:
                    problematic_domains.remove(domain)
                    
            except Exception as e:
                logger.warning(f"Error scraping {url}: {str(e)}")
                results[url] = ""
                
                # Track problematic domains
                domain = urlparse(url).netloc
                problematic_domains.add(domain)
                
                # If a domain has multiple failures, reduce the number of concurrent requests to it
                if domain in problematic_domains:
                    domain_urls_remaining = [u for u in distributed_urls if urlparse(u).netloc == domain]
                    if domain_urls_remaining:
                        logger.warning(f"Domain {domain} appears problematic. Reducing request rate.")
    
    # Log summary of problematic domains
    if problematic_domains:
        logger.warning(f"Problematic domains with errors: {', '.join(problematic_domains)}")
    
    return results

def extract_keywords(text: str) -> List[str]:
    """
    Extract meaningful keywords from text by removing common stop words.
    
    Args:
        text: The text to extract keywords from
        
    Returns:
        A list of lowercase keywords
    """
    # Simple list of stop words (could be expanded)
    stop_words = {
        'le', 'la', 'les', 'un', 'une', 'des', 'et', 'ou', 'de', 'du', 'en', 'a', 'au', 'aux',
        'par', 'pour', 'avec', 'sans', 'dans', 'sur', 'sous', 'entre', 'vers', 'chez',
        'the', 'a', 'an', 'and', 'or', 'of', 'to', 'in', 'on', 'at', 'by', 'for', 'with',
        'without', 'from', 'as', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
        'this', 'that', 'these', 'those', 'it', 'its', 'it\'s', 'they', 'them', 'their'
    }
    
    # Handle None values
    if text is None:
        return []
    
    # Remove special characters and convert to lowercase
    text = re.sub(r'[^\w\s]', ' ', text.lower())
    
    # Split into words and filter out stop words and short words
    words = [word for word in text.split() if word not in stop_words and len(word) > 2]
    
    # Normalize each word (remove accents, handle plurals)
    normalized_words = [normalize_text(word) for word in words]
    
    return normalized_words

def normalize_text(text: str) -> str:
    """
    Normalize text by removing accents, converting to lowercase, and handling plurals.
    
    Args:
        text: The text to normalize
        
    Returns:
        Normalized text
    """
    if not text:
        return ""
    
    # Convert to lowercase
    text = text.lower()
    
    # Remove accents
    text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
    
    # Handle plurals (basic French and English)
    text = re.sub(r'(s|x|aux)$', '', text)
    
    return text

def calculate_optimization_score(
    title_keywords: List[str], 
    ranking_keywords: List[Tuple[str, int]], 
    title_length: int
) -> int:
    """
    Calculate a simple optimization score based on keyword presence and title length.
    
    Args:
        title_keywords: List of keywords in the title
        ranking_keywords: List of (keyword, impressions) tuples
        title_length: Length of the title in characters
        
    Returns:
        An optimization score from 0-100
    """
    score = 0
    
    # Pré-calculer les versions normalisées des mots-clés du titre (pour éviter les calculs répétés)
    normalized_title_keywords = [normalize_text(kw) for kw in title_keywords]
    normalized_title = normalize_text(" ".join(title_keywords))
    
    # Check if top keywords are in the title (up to 50 points)
    top_keywords = ranking_keywords[:5]  # Consider top 5 keywords
    keywords_in_title = 0
    missing_keywords = []
    
    # Pré-calculer les mots-clés normalisés pour les top keywords (pour éviter les calculs répétés)
    top_keywords_data = []
    for keyword, impressions in top_keywords:
        keyword_words = extract_keywords(keyword)
        normalized_keyword_words = [normalize_text(kw) for kw in keyword_words]
        normalized_keyword = normalize_text(keyword)
        top_keywords_data.append((keyword, impressions, normalized_keyword_words, normalized_keyword))
    
    # Vérifier la présence des mots-clés principaux
    for keyword, _, normalized_keyword_words, normalized_keyword in top_keywords_data:
        # Check if the entire keyword is covered, not just individual words
        keyword_covered = False
        
        # Pour les mots-clés à plusieurs mots, exiger qu'au moins 70% des mots soient présents
        if len(normalized_keyword_words) > 1:
            words_found = sum(1 for kw in normalized_keyword_words if kw in normalized_title_keywords)
            coverage_ratio = words_found / len(normalized_keyword_words) if normalized_keyword_words else 0
            keyword_covered = coverage_ratio >= 0.7
        else:
            # Pour les mots-clés à un seul mot, exiger une correspondance exacte
            keyword_covered = any(kw in normalized_title_keywords for kw in normalized_keyword_words)
        
        # Vérifier si le mot-clé normalisé apparaît comme sous-chaîne dans le titre normalisé
        if not keyword_covered and normalized_keyword in normalized_title:
            keyword_covered = True
            
        if keyword_covered:
            keywords_in_title += 1
        else:
            missing_keywords.append(keyword)
    
    # Calculer le score pour les mots-clés (jusqu'à 50 points)
    if top_keywords:
        score += (keywords_in_title / len(top_keywords)) * 50
    
    # Title length score (up to 30 points)
    if title_length <= 60:
        score += 30
    elif title_length <= 70:
        score += 20
    elif title_length <= 80:
        score += 10
    
    # Keyword density score (up to 20 points)
    if title_keywords and ranking_keywords:
        total_ranking_keywords = sum(impressions for _, impressions in ranking_keywords)
        matched_keywords_impressions = 0
        
        # Utiliser un dictionnaire pour stocker les résultats de normalisation (memoization)
        normalized_keywords_cache = {}
        
        for keyword, impressions in ranking_keywords:
            # Utiliser le cache si le mot-clé a déjà été normalisé
            if keyword in normalized_keywords_cache:
                normalized_kw_words, norm_keyword = normalized_keywords_cache[keyword]
            else:
                keyword_words = extract_keywords(keyword)
                normalized_kw_words = [normalize_text(kw) for kw in keyword_words]
                norm_keyword = normalize_text(keyword)
                normalized_keywords_cache[keyword] = (normalized_kw_words, norm_keyword)
            
            # Vérifier la présence du mot-clé
            keyword_covered = False
            
            # Pour les mots-clés à plusieurs mots
            if len(normalized_kw_words) > 1:
                words_found = sum(1 for kw in normalized_kw_words if kw in normalized_title_keywords)
                coverage_ratio = words_found / len(normalized_kw_words) if normalized_kw_words else 0
                keyword_covered = coverage_ratio >= 0.7
            else:
                keyword_covered = any(kw in normalized_title_keywords for kw in normalized_kw_words)
            
            # Vérifier comme sous-chaîne
            if not keyword_covered and norm_keyword in normalized_title:
                keyword_covered = True
                
            if keyword_covered:
                matched_keywords_impressions += impressions
        
        # Calculer le score de densité
        if total_ranking_keywords > 0:
            keyword_coverage = matched_keywords_impressions / total_ranking_keywords
            score += keyword_coverage * 20
    
    # Calculer le score final
    final_score = min(round(score), 100)
    
    # Logs détaillés (uniquement si nécessaire)
    if missing_keywords:
        logger.info(f"Missing keywords: {', '.join(missing_keywords[:3])}{'...' if len(missing_keywords) > 3 else ''}")
    
    return final_score

def generate_improved_title(
    url: str, 
    original_title: str, 
    ranking_keywords: List[Tuple[str, int]]
) -> str:
    """
    Generate an improved title suggestion based on ranking keywords.
    No brand included as Google adds it automatically.
    
    Args:
        url: The URL
        original_title: The original title
        ranking_keywords: List of (keyword, impressions) tuples
        
    Returns:
        A suggested improved title without brand
    """
    # Si pas de mots-clés, retourner le titre original
    if not ranking_keywords:
        return original_title
    
    # Extraire les mots-clés et impressions
    keywords = [kw for kw, _ in ranking_keywords]
    impressions = [imp for _, imp in ranking_keywords]
    
    # Calculer le score d'optimisation du titre actuel
    title_keywords = extract_keywords(original_title)
    current_score = calculate_optimization_score(title_keywords, ranking_keywords, len(original_title))
    
    logger.info(f"Applying semantic analysis for {url}")
    
    # Vérifier si l'API OpenAI est configurée
    from dotenv import load_dotenv
    load_dotenv()  # Charger les variables d'environnement depuis le fichier .env
    
    if os.environ.get("OPENAI_API_KEY"):
        try:
            # Utiliser une approche synchrone pour appeler la fonction asynchrone
            nest_asyncio.apply()
            
            # Créer une nouvelle boucle d'événements
            loop = asyncio.get_event_loop()
            gpt_title = loop.run_until_complete(
                generate_title_with_gpt(original_title, ranking_keywords, url, current_score)
            )
            
            logger.info(f"Titre généré par GPT: {gpt_title}")
            return gpt_title
            
        except Exception as e:
            logger.error(f"Erreur lors de la génération du titre avec GPT: {str(e)}")
            logger.info("Fallback à la méthode traditionnelle")
    else:
        logger.info("Clé API OpenAI non configurée, utilisation de la méthode traditionnelle")
    
    # Méthode traditionnelle (fallback)
    try:
        # Créer des groupes thématiques (3 groupes par défaut)
        thematic_groups = create_thematic_groups(keywords, impressions, n_clusters=3)
        
        # Sélectionner le meilleur mot-clé de chaque groupe thématique
        best_keywords = []
        for group_id, group in thematic_groups.items():
            if group:  # S'assurer que le groupe n'est pas vide
                best_keywords.append(group[0][0])  # Prendre le mot-clé avec le plus d'impressions
        
        # Utiliser la fonction create_natural_title pour générer un titre sans marque
        return create_natural_title(best_keywords, ranking_keywords, "", original_title)
                                   
    except Exception as e:
        logger.warning(f"Error during semantic title generation: {str(e)}")
        
        # Fallback simple - utiliser les top mots-clés sans marque
        top_keywords = [kw for kw, _ in ranking_keywords[:3]]
        if top_keywords:
            new_title = f"{top_keywords[0].capitalize()}"
            if len(top_keywords) > 1 and len(new_title) + len(f" - {top_keywords[1]}") <= 60:
                new_title += f" - {top_keywords[1]}"
            if len(top_keywords) > 2 and len(new_title) + len(f" {top_keywords[2]}") <= 60:
                new_title += f" {top_keywords[2]}"
                
            return new_title
        
        return original_title

async def generate_title_with_gpt(
    original_title: str, 
    keywords: List[Tuple[str, int]], 
    url: str,
    current_score: int
) -> str:
    """
    Génère un titre optimisé en utilisant l'API GPT-4o mini.
    Ne génère pas de marque car Google l'ajoute automatiquement.
    
    Args:
        original_title: Titre original
        keywords: Liste de tuples (mot-clé, impressions)
        url: URL de la page
        current_score: Score d'optimisation actuel
        
    Returns:
        Titre optimisé généré par GPT sans marque
    """
    # Extraire les mots-clés principaux (jusqu'à 5)
    top_keywords = [kw for kw, _ in keywords[:5]]
    
    # Construire le prompt sans marque
    prompt = f"""
    Optimise ce titre SEO français en incluant plus de mots-clés pertinents:
    
    Titre original: {original_title}
    Mots-clés principaux à inclure: {', '.join(top_keywords)}
    Score d'optimisation actuel: {current_score}/100
    
    Règles strictes:
    - NE PAS inclure la marque ou nom de domaine (Google l'ajoute automatiquement)
    - Inclure 2-3 mots-clés principaux de manière naturelle
    - Longueur: 50-60 caractères maximum
    - Utiliser un français naturel et grammatical
    - Si score > 80, faire seulement des améliorations mineures
    - Prioriser les mots-clés avec le plus d'impressions
    - Créer un titre accrocheur qui donne envie de cliquer
    
    Exemple de format souhaité: "Mot-clé Principal Secondaire - Action Spécifique"
    
    Nouveau titre optimisé SANS marque:
    """
    
    try:
        # Configuration du client OpenAI
        client = AsyncOpenAI(api_key=os.environ.get("OPENAI_API_KEY", ""))
        
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=60
        )
        
        new_title = response.choices[0].message.content.strip()
        
        # Nettoyer le titre (enlever les guillemets ou préfixes éventuels)
        new_title = re.sub(r'^["\']|["\']$|^Nouveau titre optimisé[:\s]*|^SANS marque[:\s]*', '', new_title).strip()
        
        # Enlever toute marque ou domaine qui pourrait avoir été ajouté
        new_title = re.sub(r'\s*[|–-]\s*[A-Z][a-z]+(?:\.[a-z]{2,4})?\s*$', '', new_title).strip()
        
        # Vérifier si le titre est vide ou trop court
        if not new_title or len(new_title) < 10:
            return original_title
            
        return new_title
        
    except Exception as e:
        logger.error(f"Erreur lors de l'appel à l'API GPT: {str(e)}")
        return original_title

def create_natural_title(keywords: List[str], ranking_keywords: List[Tuple[str, int]], brand: str, original_title: str = "") -> str:
    """
    Crée un titre naturel en mélangeant intelligemment les mots-clés.
    Optimisé pour maximiser l'inclusion de mots-clés sans marque.
    
    Args:
        keywords: Liste des mots-clés à inclure
        ranking_keywords: Liste complète des mots-clés avec leurs impressions
        brand: Nom de la marque (ignoré car Google l'ajoute automatiquement)
        original_title: Titre original (pour extraire des éléments utiles)
        
    Returns:
        Un titre naturel combinant les mots-clés, sans marque
    """
    if not keywords:
        return original_title
    
    # Analyser le titre original pour en extraire des éléments utiles
    original_parts = []
    if original_title:
        # Séparer le titre original en parties (avant et après le séparateur |)
        parts = original_title.split('|')
        if len(parts) > 0:
            original_main = parts[0].strip()
            # Extraire les éléments significatifs du titre original
            original_parts = [p for p in re.split(r'[,\-|]', original_main) if len(p.strip()) > 3]
    
    # Extraire les entités principales (noms communs) des mots-clés
    main_entities = []
    for kw in keywords[:4]:  # Augmenté à 4 pour plus de mots-clés
        words = extract_keywords(kw)
        # Considérer les premiers mots comme potentiellement des entités principales
        if words:
            main_entities.append(words[0])  # Ajouter le premier mot comme entité principale
            # Ajouter aussi les mots plus longs qui pourraient être des entités
            for word in words:
                if len(word) > 4:  # Les mots plus longs sont souvent plus spécifiques
                    main_entities.append(word)
    
    # Nettoyer et dédupliquer les entités
    main_entities = list(set([entity for entity in main_entities if len(entity) > 2]))
    
    # Créer le titre en combinant intelligemment les éléments
    title_parts = []
        
    # Commencer par le mot-clé principal (premier de la liste)
    if keywords:
        title_parts.append(keywords[0].capitalize())
    
    # Ajouter des entités complémentaires avec plus d'espace disponible (60 chars max)
    current_length = len(' '.join(title_parts))
    for entity in main_entities[1:4]:  # Augmenté à 3 entités supplémentaires max
        potential_addition = f" {entity}"
        if current_length + len(potential_addition) <= 55:  # Plus d'espace sans marque
            title_parts.append(entity)
            current_length += len(potential_addition)
    
    # Essayer d'ajouter un 2ème mot-clé complet si possible
    if len(keywords) > 1 and current_length <= 35:
        second_keyword = keywords[1].capitalize()
        if len(current_length) + len(f" - {second_keyword}") <= 60:
            title_parts.append(f"- {second_keyword}")
    
    # Assembler le titre final sans marque
    final_title = ' '.join(title_parts)
    
    return final_title

def normalize_column_name(name: str) -> str:
    """
    Normalize column names by removing spaces, special characters, and converting to lowercase.
    
    Args:
        name: The original column name
        
    Returns:
        Normalized column name
    """
    # Handle None values
    if name is None:
        return ""
    
    # Remove special characters, convert to lowercase, and replace spaces with underscores
    normalized = re.sub(r'[^\w\s]', '', name.lower()).strip().replace(' ', '_')
    return normalized

def get_column_mapping(csv_data: List[Dict[str, Any]], csv_type: str = "scraping") -> Dict[str, str]:
    """
    Create a mapping between expected column names and actual column names in the CSV.
    
    Args:
        csv_data: List of dictionaries containing CSV data
        csv_type: Type of CSV - "scraping" (default) or "with_titles"
        
    Returns:
        Dictionary mapping expected column names to actual column names
    """
    if not csv_data:
        return {}
    
    # Get the first row to examine column names
    first_row = csv_data[0]
    
    # Define expected column names and their possible variations
    expected_columns = {
        'url': ['url', 'page', 'landing page', 'landing_page', 'page url', 'page_url', 'top pages', 'top_pages'],
        'query': ['query', 'keyword', 'search term', 'search_term', 'search query', 'search_query', 'queries', 'top queries'],
        'impressions': ['impressions', 'impr', 'impression', 'views', 'imprs'],
        'clicks': ['clicks', 'click', 'clics', 'clic'],
        'ctr': ['ctr', 'click through rate', 'click_through_rate', 'clickthrough', 'click-through rate'],
        'position': ['position', 'pos', 'ranking', 'rank', 'average position', 'avg position', 'avg_position'],
        'title': ['title', 'page title', 'page_title', 'meta title', 'meta_title', 'balise title', 'title tag'],
        'h1': ['h1', 'heading 1', 'heading_1', 'h1 tag', 'h1_tag', 'main heading', 'main_heading', 'titre h1', 'titre_h1']
    }
    
    # Create mapping
    column_mapping = {}
    
    # Print available columns for debugging
    logger.info(f"Available columns in CSV: {list(first_row.keys())}")
    
    # Try direct matching first
    for expected_col, variations in expected_columns.items():
        for col in first_row.keys():
            if col in variations or normalize_column_name(col) in [normalize_column_name(v) for v in variations]:
                column_mapping[expected_col] = col
                break
    
    # If direct matching fails, try fuzzy matching with normalized column names
    if len(column_mapping) < 3:  # We need at least url, query, and impressions
        normalized_columns = {normalize_column_name(col): col for col in first_row.keys() if col is not None}
        
        for expected_col, variations in expected_columns.items():
            if expected_col not in column_mapping:  # Only check if not already mapped
                for variation in variations:
                    normalized_variation = normalize_column_name(variation)
                    for norm_col, orig_col in normalized_columns.items():
                        if normalized_variation in norm_col or norm_col in normalized_variation:
                            column_mapping[expected_col] = orig_col
                            break
                    if expected_col in column_mapping:
                        break
    
    # Print the mapping for debugging
    logger.info(f"Column mapping: {column_mapping}")
    
    return column_mapping

def group_urls_by_domain(urls: List[str], max_per_domain: int = 5) -> Dict[str, List[str]]:
    """
    Group URLs by domain to avoid hammering a single domain with too many requests.
    
    Args:
        urls: List of URLs to group
        max_per_domain: Maximum number of URLs per domain in each group
        
    Returns:
        Dictionary mapping domain to list of URLs
    """
    domain_urls = defaultdict(list)
    
    for url in urls:
        try:
            parsed_url = urlparse(url)
            domain = parsed_url.netloc
            domain_urls[domain].append(url)
        except Exception:
            continue
    
    return domain_urls

def chunk_urls(urls: List[str], chunk_size: int = 50) -> List[List[str]]:
    """
    Split URLs into chunks for batch processing.
    
    Args:
        urls: List of URLs to chunk
        chunk_size: Maximum size of each chunk
        
    Returns:
        List of URL chunks
    """
    return [urls[i:i + chunk_size] for i in range(0, len(urls), chunk_size)]

def analyze_csv(csv_data: List[Dict[str, Any]], csv_type: str = "scraping") -> List[Dict[str, Any]]:
    """
    Analyze the CSV data from Google Search Console.
    
    Args:
        csv_data: List of dictionaries containing GSC data
        csv_type: Type of CSV - "scraping" (default) or "with_titles"
        
    Returns:
        List of dictionaries with analysis results
    """
    # Check if Gemini is configured and should be used
    from dotenv import load_dotenv
    load_dotenv()
    
    gemini_api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    openai_api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    
    logger.info("=== DIAGNOSTIC CHOIX DE MÉTHODE ===")
    logger.info(f"GEMINI_API_KEY définie: {'Oui' if gemini_api_key else 'Non'}")
    logger.info(f"GEMINI_API_KEY longueur: {len(gemini_api_key) if gemini_api_key else 0} caractères")
    logger.info(f"OPENAI_API_KEY définie: {'Oui' if openai_api_key else 'Non'}")
    logger.info(f"OPENAI_API_KEY longueur: {len(openai_api_key) if openai_api_key else 0} caractères")
    
    # PRIORITÉ À GEMINI - Force l'utilisation si la clé est définie
    if gemini_api_key:
        logger.info("🚀 CHOIX: Gemini batch processing (PRIORITÉ)")
        logger.info("📊 Avantages: Traitement par batch, économie de requêtes, contexte 1M tokens")
        return analyze_csv_with_gemini_batch(csv_data, csv_type)
    elif openai_api_key:
        logger.info("🔄 CHOIX: OpenAI traitement individuel (fallback)")
        logger.info("⚠️ Note: Considérez utiliser Gemini pour de meilleures performances")
        return analyze_csv_traditional(csv_data, csv_type)
    else:
        logger.info("🛠️ CHOIX: Méthode traditionnelle (aucune API IA configurée)")
        logger.info("💡 Conseil: Configurez GEMINI_API_KEY pour de meilleurs résultats")
        return analyze_csv_traditional(csv_data, csv_type)

def analyze_csv_with_gemini_batch(csv_data: List[Dict[str, Any]], csv_type: str = "scraping") -> List[Dict[str, Any]]:
    """
    Analyze CSV data using Gemini batch processing for title generation.
    """
    if not csv_data:
        raise ValueError("CSV data is empty")
    
    # Get column mapping
    column_mapping = get_column_mapping(csv_data, csv_type)
    
    # Check if required columns are present
    required_columns = ['url', 'query', 'impressions']
    if csv_type == "with_titles":
        required_columns.append('title')
    
    missing_columns = [col for col in required_columns if col not in column_mapping]
    
    if missing_columns:
        missing_cols_str = ', '.join(missing_columns)
        available_cols = ', '.join(csv_data[0].keys()) if csv_data else "None"
        raise ValueError(f"Required column(s) not found in CSV: {missing_cols_str}. Available columns: {available_cols}")
    
    # Group by URL and aggregate data (same as traditional method)
    url_data = defaultdict(list)
    url_titles = {}
    logger.info(f"Processing {len(csv_data)} rows from CSV (type: {csv_type})")
    
    for row in csv_data:
        # Get values using the mapped column names
        url_raw = row[column_mapping['url']]
        query = row[column_mapping['query']]
        
        # Clean URL if it contains semicolons
        url = url_raw.split(';')[0] if ';' in url_raw else url_raw
        
        # Get title if provided in CSV
        if csv_type == "with_titles" and 'title' in column_mapping:
            title = row[column_mapping['title']]
            url_titles[url] = title
        
        # Convert impressions to integer
        try:
            impressions_str = row[column_mapping['impressions']]
            if ';' in impressions_str and not impressions_str.isdigit():
                parts = url_raw.split(';')
                if len(parts) >= 3:
                    impressions = int(float(parts[2].strip()))
                else:
                    impressions = 0
            else:
                impressions = int(float(impressions_str))
        except (ValueError, TypeError, IndexError):
            impressions = 0
        
        # Extract other metrics (clicks, position, CTR)
        clicks = 0
        if 'clicks' in column_mapping:
            try:
                clicks_str = row[column_mapping['clicks']]
                clicks = int(float(clicks_str)) if clicks_str else 0
            except (ValueError, TypeError):
                clicks = 0
        
        position = 0.0
        if 'position' in column_mapping:
            try:
                position_str = row[column_mapping['position']]
                position = float(position_str) if position_str else 0.0
            except (ValueError, TypeError):
                position = 0.0
        
        ctr = 0.0
        if 'ctr' in column_mapping:
            try:
                ctr_str = row[column_mapping['ctr']]
                if isinstance(ctr_str, str) and '%' in ctr_str:
                    ctr = float(ctr_str.replace('%', '')) / 100
                else:
                    ctr = float(ctr_str) if ctr_str else 0.0
            except (ValueError, TypeError):
                ctr = 0.0
        
        # Store keyword data with all metrics
        keyword_data = {
            'keyword': query,
            'impressions': impressions,
            'clicks': clicks,
            'position': position,
            'ctr': ctr
        }
        
        url_data[url].append(keyword_data)
    
    # Filter out invalid URLs
    valid_urls = [url for url in url_data.keys() if url and url.startswith(('http://', 'https://'))]
    logger.info(f"Found {len(valid_urls)} unique valid URLs to analyze")
    
    # Get titles based on CSV type
    if csv_type == "with_titles":
        logger.info("Using titles from CSV (no scraping needed)")
        title_data = url_titles
        h1_data = {}  # H1 from CSV if provided, empty otherwise
        # Extract H1 from CSV if available
        for url in valid_urls:
            if 'h1' in column_mapping:
                # Get H1 from CSV data - need to find the row for this URL
                for row in csv_data:
                    url_from_row = row[column_mapping['url']].split(';')[0] if ';' in row[column_mapping['url']] else row[column_mapping['url']]
                    if url_from_row == url:
                        h1_data[url] = row[column_mapping['h1']] if row[column_mapping['h1']] else ""
                        break
                else:
                    h1_data[url] = ""
            else:
                h1_data[url] = ""
    else:
        # Use threaded scraping for titles and H1
        logger.info("Starting threaded title + H1 scraping process")
        scraped_data = scrape_titles_and_h1_threaded(valid_urls)
        title_data = {url: data['title'] for url, data in scraped_data.items()}
        h1_data = {url: data['h1'] for url, data in scraped_data.items()}
    
    # Process each URL
    results = []
    logger.info("Analyzing titles and keywords")
    
    # Prepare data for batch processing
    batch_data = []
    
    for url in valid_urls:
        # Get title from scraped data or CSV
        title = title_data.get(url, "")
        
        # Get H1 from scraped data or CSV
        h1 = h1_data.get(url, "")
        
        # Skip URLs without titles
        if not title and csv_type == "with_titles":
            logger.warning(f"No title found for URL: {url}")
            continue
        
        # Get keywords for this URL
        keywords_data = url_data[url]
        
        # Sort keywords by priority
        def keyword_priority_score(kw_data):
            clicks_weight = kw_data['clicks'] * 10
            impressions_weight = kw_data['impressions'] * 1
            position_weight = max(0, (21 - kw_data['position'])) if kw_data['position'] > 0 else 0
            return clicks_weight + impressions_weight + position_weight
        
        sorted_keywords_data = sorted(keywords_data, key=keyword_priority_score, reverse=True)
        
        # Prepare top keywords with metrics
        top_keywords_with_metrics = []
        for kw_data in sorted_keywords_data[:10]:
            top_keywords_with_metrics.append({
                'keyword': kw_data['keyword'],
                'impressions': kw_data['impressions'],
                'clicks': kw_data['clicks'],
                'position': round(kw_data['position'], 1) if kw_data['position'] > 0 else 0,
                'ctr': round(kw_data['ctr'] * 100, 2) if kw_data['ctr'] > 0 else 0
            })
        
        # Calculate optimization score
        title_keywords = extract_keywords(title)
        score = calculate_optimization_score_enhanced(title_keywords, sorted_keywords_data, len(title))
        
        # Add to batch data
        batch_item = {
            'url': url,
            'original_title': title,
            'h1': h1,  # Include H1 in the batch data
            'top_keywords': top_keywords_with_metrics,
            'optimization_score': score,
            'keywords_data': sorted_keywords_data,  # Keep for result building
        }
        batch_data.append(batch_item)
    
    # Process titles with Gemini batch
    logger.info(f"🚀 GEMINI BATCH: Processing {len(batch_data)} titles with batch size 100")
    logger.info(f"📈 ÉCONOMIE: {len(batch_data)} titres = {(len(batch_data) + 99) // 100} requêtes au lieu de {len(batch_data)}")
    nest_asyncio.apply()
    loop = asyncio.get_event_loop()
    gemini_results = loop.run_until_complete(
        generate_titles_batch_with_gemini(batch_data, batch_size=100)
    )
    logger.info(f"✅ GEMINI BATCH: Traitement terminé, {len(gemini_results)} titres générés")
    
    # Build final results
    results = []
    for batch_item in batch_data:
        url = batch_item['url']
        title = batch_item['original_title']
        
        # Get improved title and H1 from Gemini results or fallback
        gemini_data = gemini_results.get(url, {})
        improved_title = gemini_data.get('title', title)
        suggested_h1 = gemini_data.get('h1', "")
        
        if not improved_title or improved_title == title:
            # Fallback to traditional method if Gemini failed
            improved_title = generate_improved_title_enhanced(url, title, batch_item['keywords_data'])
        
        # Calculate missing keywords
        title_keywords = extract_keywords(title)
        missing_keywords = []
        for kw_data in batch_item['keywords_data'][:5]:
            keyword = kw_data['keyword']
            keyword_words = extract_keywords(keyword)
            if not any(kw in title_keywords for kw in keyword_words):
                missing_keywords.append(keyword)
        
        # Calculate title pixel width
        title_pixel_width = calculate_title_pixel_width(title)
        title_too_wide = title_pixel_width > 600
        
        # Create result entry
        result = {
            'url': url,
            'original_title': title,
            'current_title': title,
            'h1': batch_item['h1'],  # Include current H1 in results
            'suggested_h1': suggested_h1,  # Include suggested H1
            'title_length': len(title),
            'title_pixel_width': title_pixel_width,
            'title_too_wide': title_too_wide,
            'title_too_long': title_too_wide,
            'top_keywords': batch_item['top_keywords'],
            'missing_keywords': missing_keywords,
            'optimization_score': batch_item['optimization_score'],
            'suggested_title': improved_title,
            'improved_title': improved_title,
            'data_source': 'csv_titles' if csv_type == "with_titles" else 'scraped',
            'ai_provider': 'gemini_batch'  # Track which AI was used
        }
        
        results.append(result)
    
    logger.info(f"Batch analysis complete. Generated results for {len(results)} URLs using Gemini")
    return results

def analyze_csv_traditional(csv_data: List[Dict[str, Any]], csv_type: str = "scraping") -> List[Dict[str, Any]]:
    """
    Traditional analyze CSV method (original implementation).
    """
    if not csv_data:
        raise ValueError("CSV data is empty")
    
    # Get column mapping
    column_mapping = get_column_mapping(csv_data, csv_type)
    
    # Check if required columns are present
    required_columns = ['url', 'query', 'impressions']
    if csv_type == "with_titles":
        required_columns.append('title')
    
    missing_columns = [col for col in required_columns if col not in column_mapping]
    
    if missing_columns:
        missing_cols_str = ', '.join(missing_columns)
        available_cols = ', '.join(csv_data[0].keys()) if csv_data else "None"
        raise ValueError(f"Required column(s) not found in CSV: {missing_cols_str}. Available columns: {available_cols}")
    
    # Group by URL and aggregate data
    url_data = defaultdict(list)
    url_titles = {}  # Store titles if provided in CSV
    logger.info(f"Processing {len(csv_data)} rows from CSV (type: {csv_type})")
    
    for row in csv_data:
        # Get values using the mapped column names
        url_raw = row[column_mapping['url']]
        query = row[column_mapping['query']]
        
        # Clean URL if it contains semicolons (which might include the query)
        url = url_raw.split(';')[0] if ';' in url_raw else url_raw
        
        # Get title if provided in CSV
        if csv_type == "with_titles" and 'title' in column_mapping:
            title = row[column_mapping['title']]
            url_titles[url] = title
        
        # Convert impressions to integer
        try:
            impressions_str = row[column_mapping['impressions']]
            # If impressions is part of a combined string with semicolons, extract it
            if ';' in impressions_str and not impressions_str.isdigit():
                parts = url_raw.split(';')
                if len(parts) >= 3:  # URL;Query;Impressions;...
                    impressions = int(float(parts[2].strip()))
                else:
                    impressions = 0
            else:
                impressions = int(float(impressions_str))
        except (ValueError, TypeError, IndexError):
            impressions = 0
        
        # Extract clicks if available
        clicks = 0
        if 'clicks' in column_mapping:
            try:
                clicks_str = row[column_mapping['clicks']]
                clicks = int(float(clicks_str)) if clicks_str else 0
            except (ValueError, TypeError):
                clicks = 0
        
        # Extract position if available
        position = 0.0
        if 'position' in column_mapping:
            try:
                position_str = row[column_mapping['position']]
                position = float(position_str) if position_str else 0.0
            except (ValueError, TypeError):
                position = 0.0
        
        # Extract CTR if available
        ctr = 0.0
        if 'ctr' in column_mapping:
            try:
                ctr_str = row[column_mapping['ctr']]
                # Handle percentage format (e.g., "5.2%" or "0.052")
                if isinstance(ctr_str, str) and '%' in ctr_str:
                    ctr = float(ctr_str.replace('%', '')) / 100
                else:
                    ctr = float(ctr_str) if ctr_str else 0.0
            except (ValueError, TypeError):
                ctr = 0.0
        
        # Store keyword data with all metrics
        keyword_data = {
            'keyword': query,
            'impressions': impressions,
            'clicks': clicks,
            'position': position,
            'ctr': ctr
        }
        
        url_data[url].append(keyword_data)
    
    # Filter out invalid URLs
    valid_urls = [url for url in url_data.keys() if url and url.startswith(('http://', 'https://'))]
    logger.info(f"Found {len(valid_urls)} unique valid URLs to analyze")
    
    # Get titles based on CSV type
    if csv_type == "with_titles":
        logger.info("Using titles from CSV (no scraping needed)")
        title_data = url_titles
        h1_data = {}  # H1 from CSV if provided, empty otherwise
        # Extract H1 from CSV if available
        for url in valid_urls:
            if 'h1' in column_mapping:
                # Get H1 from CSV data - need to find the row for this URL
                for row in csv_data:
                    url_from_row = row[column_mapping['url']].split(';')[0] if ';' in row[column_mapping['url']] else row[column_mapping['url']]
                    if url_from_row == url:
                        h1_data[url] = row[column_mapping['h1']] if row[column_mapping['h1']] else ""
                        break
                else:
                    h1_data[url] = ""
            else:
                h1_data[url] = ""
    else:
        # Use threaded scraping for titles and H1
        logger.info("Starting threaded title + H1 scraping process")
        scraped_data = scrape_titles_and_h1_threaded(valid_urls)
        title_data = {url: data['title'] for url, data in scraped_data.items()}
        h1_data = {url: data['h1'] for url, data in scraped_data.items()}
    
    # Process each URL
    results = []
    logger.info("Analyzing titles and keywords")
    
    # Prepare data for batch processing
    batch_data = []
    
    for url in valid_urls:
        # Get title from scraped data or CSV
        title = title_data.get(url, "")
        
        # Get H1 from scraped data or CSV
        h1 = h1_data.get(url, "")
        
        # Skip URLs without titles
        if not title and csv_type == "with_titles":
            logger.warning(f"No title found for URL: {url}")
            continue
        
        # Get keywords for this URL
        keywords_data = url_data[url]
        
        # Sort keywords by a composite score (impressions * clicks weight + position weight)
        def keyword_priority_score(kw_data):
            # Priority based on business impact: clicks > impressions > position
            clicks_weight = kw_data['clicks'] * 10  # High weight for actual clicks
            impressions_weight = kw_data['impressions'] * 1  # Base volume
            position_weight = max(0, (21 - kw_data['position'])) if kw_data['position'] > 0 else 0  # Better position = higher score
            return clicks_weight + impressions_weight + position_weight
        
        sorted_keywords_data = sorted(keywords_data, key=keyword_priority_score, reverse=True)
        
        # Extract top keywords with their metrics for display
        top_keywords_with_metrics = []
        for kw_data in sorted_keywords_data[:10]:  # Keep top 10 for analysis
            top_keywords_with_metrics.append({
                'keyword': kw_data['keyword'],
                'impressions': kw_data['impressions'],
                'clicks': kw_data['clicks'],
                'position': round(kw_data['position'], 1) if kw_data['position'] > 0 else 0,
                'ctr': round(kw_data['ctr'] * 100, 2) if kw_data['ctr'] > 0 else 0  # Convert to percentage
            })
        
        # Convert to old format for backward compatibility with existing functions
        sorted_keywords = [(kw_data['keyword'], kw_data['impressions']) for kw_data in sorted_keywords_data]
        
        # Extract keywords from title
        title_keywords = extract_keywords(title)
        
        # Check if main keywords are in title (using enhanced logic)
        missing_keywords = []
        for kw_data in sorted_keywords_data[:5]:  # Check top 5 keywords
            keyword = kw_data['keyword']
            keyword_words = extract_keywords(keyword)
            if not any(kw in title_keywords for kw in keyword_words):
                missing_keywords.append(keyword)
        
        # Calculate optimization score using enhanced metrics
        score = calculate_optimization_score_enhanced(title_keywords, sorted_keywords_data, len(title))
        
        # Generate improved title suggestion
        improved_title = generate_improved_title_enhanced(url, title, sorted_keywords_data)
        
        # Calculate title pixel width for display warning
        title_pixel_width = calculate_title_pixel_width(title)
        title_too_wide = title_pixel_width > 600
        
        # Create result entry with enhanced keyword data
        result = {
            'url': url,
            'original_title': title,  # Renamed for clarity
            'current_title': title,   # Keep for backward compatibility
            'h1': h1,  # Include H1 in results
            'title_length': len(title),
            'title_pixel_width': title_pixel_width,
            'title_too_wide': title_too_wide,
            'title_too_long': title_too_wide,  # Keep for backward compatibility
            'top_keywords': top_keywords_with_metrics,  # Enhanced with metrics
            'missing_keywords': missing_keywords,
            'optimization_score': score,
            'suggested_title': improved_title,
            'improved_title': improved_title,  # Alias for consistency
            'data_source': 'csv_titles' if csv_type == "with_titles" else 'scraped',  # Track data source
            'ai_provider': 'openai_individual'  # Track which AI was used
        }
        
        results.append(result)
    
    logger.info(f"Traditional analysis complete. Generated results for {len(results)} URLs")
    return results

def generate_report(analysis_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Generate a summary report from the analysis results.
    
    Args:
        analysis_results: List of dictionaries with analysis results
        
    Returns:
        Dictionary with report summary
    """
    total_urls = len(analysis_results)
    urls_with_wide_titles = sum(1 for result in analysis_results if result.get('title_too_wide', False))
    urls_missing_keywords = sum(1 for result in analysis_results if len(result['missing_keywords']) > 0)
    
    if total_urls > 0:
        avg_score = sum(result['optimization_score'] for result in analysis_results) / total_urls
    else:
        avg_score = 0
    
    return {
        'total_urls': total_urls,
        'urls_with_long_titles': urls_with_wide_titles,  # Keep name for backward compatibility
        'urls_with_wide_titles': urls_with_wide_titles,
        'urls_missing_keywords': urls_missing_keywords,
        'average_optimization_score': round(avg_score, 1)
    }

def find_semantic_clusters(keywords: List[str], threshold: float = 0.75) -> List[List[str]]:
    """
    Regroupe les mots-clés en clusters sémantiques en utilisant Sentence Transformers
    
    Args:
        keywords: Liste de mots-clés à regrouper
        threshold: Seuil de similarité (entre 0 et 1)
        
    Returns:
        Liste de clusters (chaque cluster étant une liste de mots-clés)
    """
    if not keywords:
        return []
    
    # Obtenir le modèle
    model = get_sentence_model()
    
    # Encoder tous les mots-clés
    embeddings = model.encode(keywords)
    
    # Calculer la matrice de similarité
    similarity_matrix = util.pytorch_cos_sim(embeddings, embeddings).numpy()
    
    # Identifier les clusters
    clusters = []
    used_keywords = set()
    
    for i, keyword in enumerate(keywords):
        if keyword in used_keywords:
            continue
            
        cluster = [keyword]
        used_keywords.add(keyword)
        
        # Trouver les mots-clés similaires
        for j, similar_keyword in enumerate(keywords):
            if i != j and similar_keyword not in used_keywords:
                if similarity_matrix[i][j] > threshold:
                    cluster.append(similar_keyword)
                    used_keywords.add(similar_keyword)
        
        clusters.append(cluster)
    
    return clusters

def create_thematic_groups(keywords: List[str], impressions: List[int], n_clusters: int = 3) -> Dict[int, List[Tuple[str, int]]]:
    """
    Crée des groupes thématiques à partir des mots-clés en utilisant le clustering
    
    Args:
        keywords: Liste de mots-clés
        impressions: Liste d'impressions correspondant aux mots-clés
        n_clusters: Nombre de clusters à créer
        
    Returns:
        Dictionnaire de groupes thématiques avec les mots-clés et leurs impressions
    """
    if not keywords or len(keywords) < n_clusters:
        return {0: [(kw, imp) for kw, imp in zip(keywords, impressions)]}
    
    # Obtenir le modèle
    model = get_sentence_model()
    
    # Encoder les mots-clés
    embeddings = model.encode(keywords)
    
    # Appliquer le clustering hiérarchique
    clustering = AgglomerativeClustering(n_clusters=min(n_clusters, len(keywords)))
    cluster_labels = clustering.fit_predict(embeddings)
    
    # Organiser les résultats
    thematic_groups = {}
    for i, label in enumerate(cluster_labels):
        if label not in thematic_groups:
            thematic_groups[label] = []
        thematic_groups[label].append((keywords[i], impressions[i]))
    
    # Trier chaque groupe par nombre d'impressions
    for label in thematic_groups:
        thematic_groups[label].sort(key=lambda x: x[1], reverse=True)
    
    return thematic_groups

_model = None

def get_sentence_model():
    """
    Get the sentence transformer model with lazy loading
    """
    global _model
    if _model is None:
        logger.info("Loading sentence transformer model...")
        _model = SentenceTransformer('paraphrase-multilingual-mpnet-base-v2')
        logger.info("Model loaded successfully")
    return _model

def calculate_optimization_score_enhanced(
    title_keywords: List[str], 
    ranking_keywords_data: List[Dict[str, Any]], 
    title_length: int
) -> int:
    """
    Calculate an enhanced optimization score using comprehensive keyword metrics.
    
    Args:
        title_keywords: Keywords extracted from the title
        ranking_keywords_data: List of keyword data dictionaries with metrics
        title_length: Length of the title
        
    Returns:
        Optimization score from 0 to 100
    """
    if not ranking_keywords_data:
        return 0
    
    score = 0
    max_score = 100
    
    # 1. Keyword inclusion score (40 points max)
    keyword_score = 0
    total_business_value = 0
    matched_business_value = 0
    
    for kw_data in ranking_keywords_data[:10]:  # Analyze top 10 keywords
        keyword = kw_data['keyword']
        clicks = kw_data['clicks']
        impressions = kw_data['impressions']
        position = kw_data['position']
        
        # Calculate business value (clicks are most important, then impressions, then position potential)
        business_value = clicks * 10 + impressions * 1 + max(0, (21 - position)) if position > 0 else 0
        total_business_value += business_value
        
        # Check if keyword is represented in title
        keyword_words = extract_keywords(keyword)
        if any(normalize_text(kw) in [normalize_text(tw) for tw in title_keywords] for kw in keyword_words):
            matched_business_value += business_value
    
    # Calculate keyword inclusion score
    if total_business_value > 0:
        keyword_score = (matched_business_value / total_business_value) * 40
    
    score += keyword_score
    
    # 2. Title length optimization (20 points max)
    if 45 <= title_length <= 60:
        length_score = 20  # Optimal length
    elif 35 <= title_length < 45:
        length_score = 15  # Good length
    elif 25 <= title_length < 35:
        length_score = 10  # Acceptable but short
    elif title_length < 25:
        length_score = 5   # Too short
    elif 60 < title_length <= 70:
        length_score = 10  # A bit too long
    else:
        length_score = 0   # Too long
    
    score += length_score
    
    # 3. High-value keyword prioritization (25 points max)
    priority_score = 0
    if ranking_keywords_data:
        top_3_keywords = ranking_keywords_data[:3]
        for i, kw_data in enumerate(top_3_keywords):
            keyword = kw_data['keyword']
            keyword_words = extract_keywords(keyword)
            weight = [15, 7, 3][i]  # Decreasing weights for top 3
            
            if any(normalize_text(kw) in [normalize_text(tw) for tw in title_keywords] for kw in keyword_words):
                priority_score += weight
    
    score += priority_score
    
    # 4. Readability and structure (15 points max)
    structure_score = 0
    
    # Check for natural language patterns
    if title_length > 0:
        # Avoid keyword stuffing (penalize repeated words)
        words = title_keywords
        unique_words = set(words)
        if len(words) > 0:
            uniqueness_ratio = len(unique_words) / len(words)
            structure_score += uniqueness_ratio * 8
        
        # Bonus for proper capitalization and structure
        if any(word[0].isupper() for word in title_keywords if word):
            structure_score += 3
        
        # Bonus for brand separation (| or -)
        if any(sep in ' '.join(title_keywords) for sep in ['|', '-']):
            structure_score += 4
    
    score += min(15, structure_score)
    
    return min(max_score, int(score))

def generate_improved_title_enhanced(
    url: str, 
    original_title: str, 
    ranking_keywords_data: List[Dict[str, Any]]
) -> str:
    """
    Generate an enhanced title suggestion using comprehensive keyword data and business intelligence.
    
    Args:
        url: The URL
        original_title: The original title
        ranking_keywords_data: List of keyword data dictionaries with comprehensive metrics
        
    Returns:
        An improved title suggestion (without brand as Google adds it automatically)
    """
    if not ranking_keywords_data:
        return original_title
    
    # Calculate current title score
    title_keywords = extract_keywords(original_title)
    current_score = calculate_optimization_score_enhanced(title_keywords, ranking_keywords_data, len(original_title))
    
    # If current title is already excellent (>85), make minimal changes
    if current_score > 85:
        return optimize_excellent_title(original_title, ranking_keywords_data)
    
    # Analyze keyword strategy
    strategy = analyze_keyword_strategy(ranking_keywords_data)
    
    # Generate title based on strategy
    if strategy['type'] == 'high_traffic':
        return generate_traffic_focused_title(ranking_keywords_data, original_title)
    elif strategy['type'] == 'conversion_focused':
        return generate_conversion_focused_title(ranking_keywords_data, original_title)
    else:
        return generate_balanced_title(ranking_keywords_data, original_title)

def analyze_keyword_strategy(ranking_keywords_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Analyze the keyword data to determine the best optimization strategy.
    """
    if not ranking_keywords_data:
        return {'type': 'balanced', 'focus': []}
    
    total_clicks = sum(kw['clicks'] for kw in ranking_keywords_data[:10])
    total_impressions = sum(kw['impressions'] for kw in ranking_keywords_data[:10])
    avg_position = sum(kw['position'] for kw in ranking_keywords_data[:10] if kw['position'] > 0) / len([kw for kw in ranking_keywords_data[:10] if kw['position'] > 0]) if any(kw['position'] > 0 for kw in ranking_keywords_data[:10]) else 0
    
    # High traffic, low conversion (focus on impressions)
    if total_impressions > 1000 and total_clicks < total_impressions * 0.02:
        return {'type': 'high_traffic', 'focus': ranking_keywords_data[:3]}
    
    # Good conversion rate (focus on clicks)
    elif total_clicks > total_impressions * 0.05:
        return {'type': 'conversion_focused', 'focus': ranking_keywords_data[:3]}
    
    # Balanced approach
    else:
        return {'type': 'balanced', 'focus': ranking_keywords_data[:5]}

def optimize_excellent_title(original_title: str, ranking_keywords_data: List[Dict[str, Any]]) -> str:
    """
    Make minimal optimizations to an already excellent title.
    Focus on length optimization and keyword inclusion without brand.
    """
    # For excellent titles, prioritize keyword inclusion over length if possible
    if len(original_title) > 65:
        # Try to optimize while keeping important keywords
        title_keywords = extract_keywords(original_title)
        important_keywords = [kw['keyword'] for kw in ranking_keywords_data[:3]]
        
        # Split by common separators
        parts = re.split(r'[|–-]', original_title)
        if len(parts) > 1:
            # Keep the part that contains more important keywords
            main_part = parts[0].strip()
            
            # Check if we can add a second important keyword
            for kw_data in ranking_keywords_data[:3]:
                keyword = kw_data['keyword']
                if keyword.lower() not in main_part.lower() and len(main_part) + len(f" {keyword}") <= 60:
                    main_part = f"{main_part} {keyword.title()}"
                    break
            
            return main_part
        else:
            # Try to shorten while preserving key terms
            words = original_title.split()
            if len(words) > 8:
                # Keep first 6-7 words and try to add an important keyword
                shortened = ' '.join(words[:7])
                for kw_data in ranking_keywords_data[:2]:
                    keyword = kw_data['keyword']
                    if keyword.lower() not in shortened.lower() and len(shortened) + len(f" {keyword}") <= 60:
                        shortened = f"{shortened} {keyword.title()}"
                        break
                return shortened
    
    # For excellent titles under 65 chars, try to add missing high-value keywords
    missing_keywords = []
    title_lower = original_title.lower()
    for kw_data in ranking_keywords_data[:3]:
        if kw_data['keyword'].lower() not in title_lower:
            missing_keywords.append(kw_data['keyword'])
    
    if missing_keywords and len(original_title) + len(f" {missing_keywords[0]}") <= 60:
        return f"{original_title} {missing_keywords[0].title()}"
    
    return original_title

def generate_traffic_focused_title(ranking_keywords_data: List[Dict[str, Any]], original_title: str) -> str:
    """
    Generate a title focused on capturing high-impression keywords.
    """
    # Sort by impressions and position potential
    traffic_keywords = sorted(ranking_keywords_data[:5], 
                            key=lambda x: x['impressions'] * (21 - x['position']) if x['position'] > 0 else x['impressions'], 
                            reverse=True)
    
    main_keyword = traffic_keywords[0]['keyword'] if traffic_keywords else ""
    
    # Extract key terms that appear in multiple keywords
    all_words = []
    for kw_data in traffic_keywords:
        all_words.extend(extract_keywords(kw_data['keyword']))
    
    # Find common terms
    word_counts = {}
    for word in all_words:
        word_counts[word] = word_counts.get(word, 0) + 1
    
    common_terms = [word for word, count in word_counts.items() if count >= 2 and len(word) > 3]
    
    # Build title
    title_parts = []
    if main_keyword:
        title_parts.append(main_keyword.title())
    
    # Add complementary terms if space allows
    current_length = len(' '.join(title_parts))
    for term in common_terms[:2]:
        if term.lower() not in main_keyword.lower():
            addition = f" {term.title()}"
            if current_length + len(addition) <= 60:
                title_parts.append(term.title())
                current_length += len(addition)
    
    main_title = ' '.join(title_parts)
    
    return main_title

def generate_conversion_focused_title(ranking_keywords_data: List[Dict[str, Any]], original_title: str) -> str:
    """
    Generate a title focused on keywords that already convert well.
    """
    # Sort by click performance and CTR
    conversion_keywords = sorted(ranking_keywords_data[:5], 
                               key=lambda x: x['clicks'] * (x['ctr'] + 1), 
                               reverse=True)
    
    if not conversion_keywords:
        return original_title
    
    # Use the best converting keyword as primary
    primary_keyword = conversion_keywords[0]['keyword']
    
    # Look for action-oriented terms in converting keywords
    action_terms = []
    for kw_data in conversion_keywords:
        words = extract_keywords(kw_data['keyword'])
        for word in words:
            if word.lower() in ['achat', 'acheter', 'prix', 'vente', 'commande', 'livraison', 'gratuit', 'promo', 'solde']:
                action_terms.append(word)
    
    title_parts = [primary_keyword.title()]
    
    # Add action terms if they improve conversion potential
    current_length = len(primary_keyword)
    for term in action_terms[:1]:  # Add at most one action term
        addition = f" {term.title()}"
        if current_length + len(addition) <= 60:
            title_parts.append(term.title())
            current_length += len(addition)
    
    main_title = ' '.join(title_parts)
    
    return main_title

def generate_balanced_title(ranking_keywords_data: List[Dict[str, Any]], original_title: str) -> str:
    """
    Generate a balanced title optimizing for both traffic and conversion.
    """
    if not ranking_keywords_data:
        return original_title
    
    # Use business value scoring for balanced approach
    def business_value(kw_data):
        return kw_data['clicks'] * 5 + kw_data['impressions'] * 1 + max(0, (21 - kw_data['position'])) if kw_data['position'] > 0 else 0
    
    sorted_by_value = sorted(ranking_keywords_data[:7], key=business_value, reverse=True)
    
    primary_keyword = sorted_by_value[0]['keyword']
    
    # Extract semantic themes
    themes = extract_semantic_themes(sorted_by_value[:5])
    
    # Build title with primary keyword and strongest theme
    title_components = [primary_keyword.title()]
    
    if themes and len(themes) > 0:
        main_theme = themes[0]
        # Add theme terms if they're not already in primary keyword
        for term in main_theme[:2]:  # Max 2 additional terms
            if term.lower() not in primary_keyword.lower():
                current_length = len(' '.join(title_components))
                addition = f" {term.title()}"
                if current_length + len(addition) <= 60:
                    title_components.append(term.title())
    
    main_title = ' '.join(title_components)
    
    return main_title

def extract_semantic_themes(keywords_data: List[Dict[str, Any]]) -> List[List[str]]:
    """
    Extract semantic themes from keyword data.
    """
    all_words = []
    for kw_data in keywords_data:
        words = extract_keywords(kw_data['keyword'])
        all_words.extend(words)
    
    # Count word frequency
    word_counts = {}
    for word in all_words:
        if len(word) > 3:  # Only consider meaningful words
            word_counts[word] = word_counts.get(word, 0) + 1
    
    # Group words that appear together frequently
    themes = []
    common_words = [word for word, count in word_counts.items() if count >= 2]
    
    if common_words:
        themes.append(common_words[:3])  # Main theme with top 3 common words
    
    return themes

def calculate_title_pixel_width(title: str) -> int:
    """
    Calculate the approximate pixel width of a title for display in search results.
    
    Args:
        title: The title text
        
    Returns:
        Approximate pixel width
    """
    if not title:
        return 0
    
    # Approximate character widths in pixels for Google search results
    # Based on typical search result font (Arial, ~13px)
    char_widths = {
        # Narrow characters
        'i': 3, 'l': 3, 'I': 3, 'j': 3, 't': 4, 'f': 4, 'r': 4,
        # Regular characters  
        'a': 7, 'b': 7, 'c': 6, 'd': 7, 'e': 7, 'g': 7, 'h': 7, 'k': 6, 'n': 7, 'o': 7, 'p': 7, 'q': 7, 's': 6, 'u': 7, 'v': 6, 'x': 6, 'y': 6, 'z': 6,
        'A': 8, 'B': 8, 'C': 8, 'D': 8, 'E': 7, 'F': 7, 'G': 9, 'H': 8, 'J': 6, 'K': 8, 'L': 7, 'N': 8, 'O': 9, 'P': 8, 'Q': 9, 'R': 8, 'S': 8, 'T': 7, 'U': 8, 'V': 8, 'X': 8, 'Y': 8, 'Z': 7,
        # Wide characters
        'm': 11, 'w': 10, 'M': 10, 'W': 11,
        # Numbers
        '0': 7, '1': 7, '2': 7, '3': 7, '4': 7, '5': 7, '6': 7, '7': 7, '8': 7, '9': 7,
        # Special characters
        ' ': 4, '.': 3, ',': 3, '!': 3, '?': 6, ':': 3, ';': 3, '-': 4, '_': 7, '|': 3, '&': 8, '+': 7, '=': 7, 
        '(': 4, ')': 4, '[': 4, ']': 4, '{': 4, '}': 4, '/': 4, '\\': 4, '"': 4, "'": 2
    }
    
    total_width = 0
    for char in title:
        if char in char_widths:
            total_width += char_widths[char]
        else:
            # Default width for unknown characters
            total_width += 7
    
    return total_width

def analyze_dual_csv(gsc_data: List[Dict[str, Any]], url_title_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Analyze data from two separate CSV files: GSC data and URL-Title mapping.
    
    Args:
        gsc_data: List of dictionaries from Google Search Console CSV
        url_title_data: List of dictionaries with URL-Title mapping
        
    Returns:
        List of dictionaries with analysis results
    """
    if not gsc_data or not url_title_data:
        raise ValueError("Both GSC data and URL-Title data are required")
    
    logger.info(f"Processing dual CSV: {len(gsc_data)} GSC rows, {len(url_title_data)} URL-Title rows")
    
    # Get column mapping for GSC data
    gsc_column_mapping = get_column_mapping(gsc_data, csv_type="scraping")
    
    # Check required GSC columns
    required_gsc_columns = ['url', 'query', 'impressions']
    missing_gsc_columns = [col for col in required_gsc_columns if col not in gsc_column_mapping]
    
    if missing_gsc_columns:
        missing_cols_str = ', '.join(missing_gsc_columns)
        available_cols = ', '.join(gsc_data[0].keys()) if gsc_data else "None"
        raise ValueError(f"Required GSC column(s) not found: {missing_cols_str}. Available columns: {available_cols}")
    
    # Create URL-Title mapping
    url_title_mapping = {}
    
    # Try to map URL-Title data columns
    url_title_first_row = url_title_data[0] if url_title_data else {}
    url_col = None
    title_col = None
    
    # Find URL column
    for col in url_title_first_row.keys():
        if col.lower() in ['url', 'page', 'landing page', 'landing_page', 'page url', 'page_url']:
            url_col = col
            break
    
    # Find Title column
    for col in url_title_first_row.keys():
        if col.lower() in ['title', 'page title', 'page_title', 'meta title', 'meta_title', 'balise title']:
            title_col = col
            break
    
    if not url_col or not title_col:
        available_cols = ', '.join(url_title_first_row.keys()) if url_title_first_row else "None"
        raise ValueError(f"Could not find URL and Title columns in URL-Title file. Available columns: {available_cols}")
    
    logger.info(f"Using URL column: '{url_col}', Title column: '{title_col}'")
    
    # Build URL-Title mapping
    for row in url_title_data:
        url = row.get(url_col, '').strip()
        title = row.get(title_col, '').strip()
        if url and title:
            url_title_mapping[url] = title
    
    logger.info(f"Created URL-Title mapping for {len(url_title_mapping)} URLs")
    
    # Group GSC data by URL and aggregate
    url_data = defaultdict(list)
    
    for row in gsc_data:
        # Get values using the mapped column names
        url_raw = row[gsc_column_mapping['url']]
        query = row[gsc_column_mapping['query']]
        
        # Clean URL if it contains semicolons
        url = url_raw.split(';')[0] if ';' in url_raw else url_raw
        url = url.strip()
        
        # Convert impressions to integer
        try:
            impressions_str = row[gsc_column_mapping['impressions']]
            impressions = int(float(impressions_str)) if impressions_str else 0
        except (ValueError, TypeError):
            impressions = 0
        
        # Extract clicks if available
        clicks = 0
        if 'clicks' in gsc_column_mapping:
            try:
                clicks_str = row[gsc_column_mapping['clicks']]
                clicks = int(float(clicks_str)) if clicks_str else 0
            except (ValueError, TypeError):
                clicks = 0
        
        # Extract position if available
        position = 0.0
        if 'position' in gsc_column_mapping:
            try:
                position_str = row[gsc_column_mapping['position']]
                position = float(position_str) if position_str else 0.0
            except (ValueError, TypeError):
                position = 0.0
        
        # Extract CTR if available
        ctr = 0.0
        if 'ctr' in gsc_column_mapping:
            try:
                ctr_str = row[gsc_column_mapping['ctr']]
                # Handle percentage format
                if isinstance(ctr_str, str) and '%' in ctr_str:
                    ctr = float(ctr_str.replace('%', '')) / 100
                else:
                    ctr = float(ctr_str) if ctr_str else 0.0
            except (ValueError, TypeError):
                ctr = 0.0
        
        # Store keyword data with all metrics
        keyword_data = {
            'keyword': query,
            'impressions': impressions,
            'clicks': clicks,
            'position': position,
            'ctr': ctr
        }
        
        url_data[url].append(keyword_data)
    
    # Filter valid URLs that have both GSC data and titles
    valid_urls = []
    for url in url_data.keys():
        if url and url.startswith(('http://', 'https://')) and url in url_title_mapping:
            valid_urls.append(url)
    
    logger.info(f"Found {len(valid_urls)} URLs with both GSC data and titles")
    
    if not valid_urls:
        raise ValueError("No URLs found that have both GSC data and corresponding titles")
    
    # Process each URL
    results = []
    logger.info("Analyzing titles and keywords from dual CSV")
    
    for url in valid_urls:
        # Get title from mapping
        title = url_title_mapping[url]
        
        # Get keywords for this URL
        keywords_data = url_data[url]
        
        # Sort keywords by business value
        def keyword_priority_score(kw_data):
            clicks_weight = kw_data['clicks'] * 10
            impressions_weight = kw_data['impressions'] * 1
            position_weight = max(0, (21 - kw_data['position'])) if kw_data['position'] > 0 else 0
            return clicks_weight + impressions_weight + position_weight
        
        sorted_keywords_data = sorted(keywords_data, key=keyword_priority_score, reverse=True)
        
        # Extract top keywords with their metrics for display
        top_keywords_with_metrics = []
        for kw_data in sorted_keywords_data[:10]:
            top_keywords_with_metrics.append({
                'keyword': kw_data['keyword'],
                'impressions': kw_data['impressions'],
                'clicks': kw_data['clicks'],
                'position': round(kw_data['position'], 1) if kw_data['position'] > 0 else 0,
                'ctr': round(kw_data['ctr'] * 100, 2) if kw_data['ctr'] > 0 else 0
            })
        
        # Extract keywords from title
        title_keywords = extract_keywords(title)
        
        # Check missing keywords
        missing_keywords = []
        for kw_data in sorted_keywords_data[:5]:
            keyword = kw_data['keyword']
            keyword_words = extract_keywords(keyword)
            if not any(kw in title_keywords for kw in keyword_words):
                missing_keywords.append(keyword)
        
        # Calculate optimization score
        score = calculate_optimization_score_enhanced(title_keywords, sorted_keywords_data, len(title))
        
        # Generate improved title suggestion
        improved_title = generate_improved_title_enhanced(url, title, sorted_keywords_data)
        
        # Calculate title pixel width
        title_pixel_width = calculate_title_pixel_width(title)
        title_too_wide = title_pixel_width > 600
        
        # Create result entry
        result = {
            'url': url,
            'original_title': title,
            'current_title': title,
            'h1': batch_item['h1'],  # Include H1 in results
            'title_length': len(title),
            'title_pixel_width': title_pixel_width,
            'title_too_wide': title_too_wide,
            'title_too_long': title_too_wide,
            'top_keywords': top_keywords_with_metrics,
            'missing_keywords': missing_keywords,
            'optimization_score': score,
            'suggested_title': improved_title,
            'improved_title': improved_title,
            'data_source': 'dual_csv'
        }
        
        results.append(result)
    
    logger.info(f"Dual CSV analysis complete. Generated results for {len(results)} URLs")
    return results

async def generate_titles_batch_with_gemini(
    url_data_list: List[Dict[str, Any]],
    batch_size: int = 100
) -> Dict[str, Dict[str, str]]:
    """
    Génère des titres optimisés en batch avec Gemini 2.5 Flash.
    Profite de la fenêtre de contexte d'1 million de tokens pour traiter plusieurs titres simultanément.
    
    Args:
        url_data_list: Liste de dictionnaires contenant url, titre, mots-clés et score
        batch_size: Nombre de titres à traiter par batch (défaut: 20)
        
    Returns:
        Dictionnaire {url: nouveau_titre} avec les titres optimisés
    """
    # Vérifier si l'API Gemini est configurée
    from dotenv import load_dotenv
    load_dotenv()
    
    gemini_api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    logger.info(f"🔑 GEMINI SETUP: Clé API longueur = {len(gemini_api_key) if gemini_api_key else 0}")
    
    if not gemini_api_key:
        logger.error("❌ GEMINI ERREUR: Clé API Gemini non configurée!")
        logger.error("💡 SOLUTION: Ajoutez GEMINI_API_KEY=votre_clé dans le fichier .env")
        return {}
    
    logger.info(f"✅ GEMINI OK: Initialisation pour {len(url_data_list)} titres en {(len(url_data_list) + batch_size - 1) // batch_size} batches")
    
    # Configurer Gemini
    genai.configure(api_key=gemini_api_key)
    model = genai.GenerativeModel('gemini-2.0-flash-exp')
    
    results = {}
    
    # Traiter par batches pour respecter les limites
    for i in range(0, len(url_data_list), batch_size):
        batch = url_data_list[i:i + batch_size]
        
        try:
            batch_results = await process_batch_with_gemini(model, batch)
            results.update(batch_results)
            
            # Petite pause entre les batches pour éviter la limitation de débit
            if i + batch_size < len(url_data_list):
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"Erreur lors du traitement du batch {i//batch_size + 1}: {str(e)}")
            # Fallback individuel pour ce batch
            for url_data in batch:
                try:
                    individual_result = await generate_title_with_gemini_individual(model, url_data)
                    results[url_data['url']] = {
                        'title': individual_result,
                        'h1': ""
                    }
                except Exception as e2:
                    logger.error(f"Erreur fallback pour {url_data['url']}: {str(e2)}")
                    results[url_data['url']] = {
                        'title': url_data['original_title'],
                        'h1': ""
                    }
    
    return results

async def process_batch_with_gemini(model, batch: List[Dict[str, Any]]) -> Dict[str, Dict[str, str]]:
    """
    Traite un batch de titres et H1 avec Gemini en une seule requête.
    """
    # Construire le prompt batch
    batch_prompt = """
Tu es un expert SEO français. Optimise ces titres ET propose des H1 optimisés en respectant ces règles STRICTES:

RÈGLES OBLIGATOIRES POUR TITLE:
- NE JAMAIS inclure la marque/nom de domaine (Google l'ajoute automatiquement)
- Longueur: 55 caractères MINIMUM et 12 mots maximum.
- Inclure les top mots-clés principaux naturellement, essaie d'en inclure le plus possible tout en restant naturel.
- Français naturel et grammatical
- Si score > 80, ne pas modifier le titre
- Maximiser la diversité sémantique en intégrant le max de mots-clés différents
- Priorité aux mots-clés avec le plus de clicks et d'impressions

RÈGLES OBLIGATOIRES POUR H1:
- DIFFÉRENT du title (éviter la duplication)
- Peut viser des mots-clés différents ou complémentaires du title
- Prioriser les mots-clés avec le plus de clics/impressions/meilleures positions
- Français naturel et grammatical
- Longueur: 40-70 caractères idéalement
- Créer une complémentarité stratégique title/H1 selon les performances des mots-clés

FORMAT DE RÉPONSE (OBLIGATOIRE):
Pour chaque URL, réponds uniquement:
URL: [url]
TITRE: [nouveau_titre_optimisé]
H1: [nouveau_h1_optimisé]

DONNÉES À TRAITER:
"""
    
    # Ajouter chaque élément du batch
    for idx, url_data in enumerate(batch, 1):
        top_keywords = [kw['keyword'] for kw in url_data['top_keywords'][:5]]
        h1_info = f"H1 actuel: {url_data.get('h1', 'Non disponible')}" if url_data.get('h1') else "H1: Non disponible"
        batch_prompt += f"""
{idx}. URL: {url_data['url']}
   Titre actuel: {url_data['original_title']}
   {h1_info}
   Score actuel: {url_data['optimization_score']}/100
   Mots-clés principaux: {', '.join(top_keywords)}
   
"""
    
    batch_prompt += """
GÉNÈRE MAINTENANT les titres optimisés en respectant exactement le format de réponse demandé:
"""
    
    try:
        response = await model.generate_content_async(batch_prompt)
        return parse_batch_response(response.text, batch)
        
    except Exception as e:
        logger.error(f"Erreur lors de l'appel batch à Gemini: {str(e)}")
        raise

def parse_batch_response(response_text: str, batch: List[Dict[str, Any]]) -> Dict[str, Dict[str, str]]:
    """
    Parse la réponse batch de Gemini pour extraire les titres et H1 optimisés.
    """
    results = {}
    lines = response_text.split('\n')
    current_url = None
    current_data = {}
    
    for line in lines:
        line = line.strip()
        
        if line.startswith('URL:'):
            # Sauvegarder les données précédentes si elles existent
            if current_url and current_data:
                results[current_url] = current_data
            
            # Commencer une nouvelle URL
            current_url = line.replace('URL:', '').strip()
            current_data = {}
            
        elif line.startswith('TITRE:') and current_url:
            title = line.replace('TITRE:', '').strip()
            # Nettoyer le titre
            title = re.sub(r'^["\'\`]|["\'\`]$', '', title).strip()
            
            if title and len(title) >= 10:
                current_data['title'] = title
            else:
                # Fallback au titre original si le titre généré est invalide
                original_title = next((item['original_title'] for item in batch if item['url'] == current_url), '')
                current_data['title'] = original_title
                
        elif line.startswith('H1:') and current_url:
            h1 = line.replace('H1:', '').strip()
            # Nettoyer le H1
            h1 = re.sub(r'^["\'\`]|["\'\`]$', '', h1).strip()
            
            if h1 and len(h1) >= 10:
                current_data['h1'] = h1
            else:
                # Pas de H1 généré ou invalide
                current_data['h1'] = ""
    
    # Sauvegarder la dernière URL
    if current_url and current_data:
        results[current_url] = current_data
    
    # S'assurer que toutes les URLs du batch ont un résultat
    for url_data in batch:
        if url_data['url'] not in results:
            results[url_data['url']] = {
                'title': url_data['original_title'],
                'h1': ""
            }
        elif 'title' not in results[url_data['url']]:
            results[url_data['url']]['title'] = url_data['original_title']
        elif 'h1' not in results[url_data['url']]:
            results[url_data['url']]['h1'] = ""
    
    return results

async def generate_title_with_gemini_individual(model, url_data: Dict[str, Any]) -> str:
    """
    Génère un titre optimisé pour une seule URL avec Gemini (fallback).
    """
    top_keywords = [kw['keyword'] for kw in url_data['top_keywords'][:5]]
    h1_info = f"\nH1 actuel: {url_data.get('h1', 'Non disponible')}" if url_data.get('h1') else "\nH1: Non disponible"
    
    prompt = f"""
Optimise ce titre SEO français:

Titre actuel: {url_data['original_title']}{h1_info}
Mots-clés principaux: {', '.join(top_keywords)}
Score actuel: {url_data['optimization_score']}/100

Règles STRICTES:
- NE PAS inclure la marque (Google l'ajoute automatiquement)
- Inclure 2-3 mots-clés principaux naturellement
- Longueur: 50-60 caractères maximum
- Français naturel et grammatical
- Si score > 80, faire seulement des améliorations mineures
- Tenir compte du H1 pour éviter la redondance ou créer une complémentarité

Réponds uniquement avec le nouveau titre optimisé:
"""
    
    try:
        response = await model.generate_content_async(prompt)
        new_title = response.text.strip()
        
        # Nettoyer le titre
        new_title = re.sub(r'^["\'\`]|["\'\`]$', '', new_title).strip()
        
        if new_title and len(new_title) >= 10:
            return new_title
        else:
            return url_data['original_title']
            
    except Exception as e:
        logger.error(f"Erreur lors de la génération individual avec Gemini: {str(e)}")
        return url_data['original_title']

def scrape_title_and_h1_with_retry(url: str, max_retries: int = 2) -> Tuple[str, str]:
    """
    Scrape title and H1 with retry logic for resilience.
    
    Args:
        url: URL to scrape
        max_retries: Maximum number of retry attempts
        
    Returns:
        Tuple of (title, h1) or empty strings if not found
    """
    retries = 0
    while retries <= max_retries:
        try:
            # Increase timeout for each retry
            timeout = REQUEST_TIMEOUT * (retries + 1)
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            # Add a small random delay to avoid being blocked
            if retries > 0:
                delay = random.uniform(1.0, 3.0)
                time.sleep(delay)
                
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract title
            title_tag = soup.find('title')
            title = title_tag.text.strip() if title_tag and title_tag.text else ""
            
            # Extract H1
            h1_tag = soup.find('h1')
            h1 = h1_tag.text.strip() if h1_tag and h1_tag.text else ""
            
            return title, h1
            
        except requests.exceptions.Timeout:
            retries += 1
            if retries <= max_retries:
                logger.info(f"Timeout for {url}, retrying ({retries}/{max_retries})...")
            else:
                logger.warning(f"Max retries reached for {url}")
                return "", ""
                
        except requests.exceptions.HTTPError as e:
            # Don't retry for client errors (4xx)
            if 400 <= e.response.status_code < 500:
                logger.warning(f"Client error {e.response.status_code} for {url}")
                return "", ""
            
            # Retry for server errors (5xx)
            retries += 1
            if retries <= max_retries:
                logger.info(f"Server error {e.response.status_code} for {url}, retrying ({retries}/{max_retries})...")
            else:
                logger.warning(f"Max retries reached for {url}")
                return "", ""
                
        except Exception as e:
            retries += 1
            if retries <= max_retries:
                logger.info(f"Error for {url}: {str(e)}, retrying ({retries}/{max_retries})...")
            else:
                logger.warning(f"Max retries reached for {url}")
                return "", ""
    
    return "", ""

def scrape_titles_and_h1_threaded(urls: List[str], max_workers: int = MAX_WORKERS) -> Dict[str, Dict[str, str]]:
    """
    Scrape titles and H1s using multi-threading for parallel processing.
    
    Args:
        urls: List of URLs to scrape
        max_workers: Maximum number of threads to use
        
    Returns:
        Dictionary mapping URLs to {'title': title, 'h1': h1}
    """
    results = {}
    total_urls = len(urls)
    
    logger.info(f"Starting threaded scraping of {total_urls} URLs (title + H1) with {max_workers} workers")
    
    # Group URLs by domain to avoid hammering a single domain
    domain_urls = group_urls_by_domain(urls)
    
    # Track problematic domains
    problematic_domains = set()
    
    # Flatten the grouped URLs while respecting domain distribution
    distributed_urls = []
    remaining = True
    
    while remaining:
        remaining = False
        for domain, domain_url_list in list(domain_urls.items()):
            if domain_url_list:
                distributed_urls.append(domain_url_list.pop(0))
                remaining = True
    
    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_url = {executor.submit(scrape_title_and_h1_with_retry, url): url for url in distributed_urls}
        
        # Process results as they complete
        completed = 0
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                title, h1 = future.result()
                results[url] = {'title': title, 'h1': h1}
                completed += 1
                
                # Log progress every 10 URLs or at the end
                if completed % 10 == 0 or completed == total_urls:
                    logger.info(f"Scraped {completed}/{total_urls} URLs ({(completed/total_urls)*100:.1f}%)")
                
                # If we got a title, the domain is not problematic
                domain = urlparse(url).netloc
                if domain in problematic_domains and title:
                    problematic_domains.remove(domain)
                    
            except Exception as e:
                logger.warning(f"Error scraping {url}: {str(e)}")
                results[url] = {'title': "", 'h1': ""}
                
                # Track problematic domains
                domain = urlparse(url).netloc
                problematic_domains.add(domain)
                
                # If a domain has multiple failures, reduce the number of concurrent requests to it
                if domain in problematic_domains:
                    domain_urls_remaining = [u for u in distributed_urls if urlparse(u).netloc == domain]
                    if domain_urls_remaining:
                        logger.warning(f"Domain {domain} appears problematic. Reducing request rate.")
    
    # Log summary of problematic domains
    if problematic_domains:
        logger.warning(f"Problematic domains encountered: {', '.join(problematic_domains)}")
    
    logger.info(f"Completed scraping {total_urls} URLs. Found {sum(1 for r in results.values() if r['title'])} titles and {sum(1 for r in results.values() if r['h1'])} H1s")
    return results
