#!/usr/bin/env python3
"""
Script de test pour vérifier le déploiement avec sous-répertoire
"""

import os
import requests
import sys
from urllib.parse import urljoin

def test_deployment(base_url, root_path=""):
    """Test les endpoints de l'application"""
    
    # Construire l'URL de base complète
    if root_path:
        base_url = urljoin(base_url, root_path.lstrip('/') + '/')
    
    print(f"🧪 Test de déploiement sur : {base_url}")
    
    try:
        # Test 1: Page d'accueil
        response = requests.get(base_url, timeout=10)
        if response.status_code == 200:
            print("✅ Page d'accueil accessible")
        else:
            print(f"❌ Page d'accueil : {response.status_code}")
            return False
        
        # Test 2: Fichiers statiques (si accessible)
        static_url = urljoin(base_url, 'static/')
        response = requests.get(static_url, timeout=10)
        print(f"ℹ️  Statiques : {response.status_code}")
        
        # Test 3: Vérifier que la page contient les bonnes URLs
        if 'upload' in response.text and 'export' in response.text:
            print("✅ URLs relatives correctement générées")
        else:
            print("⚠️  Vérifier que les URLs sont correctement générées")
        
        return True
        
    except requests.exceptions.ConnectionError:
        print("❌ Impossible de se connecter à l'application")
        return False
    except requests.exceptions.Timeout:
        print("❌ Timeout lors de la connexion")
        return False
    except Exception as e:
        print(f"❌ Erreur : {e}")
        return False

def main():
    """Point d'entrée principal"""
    
    # Configuration par défaut
    base_url = os.getenv('TEST_BASE_URL', 'http://localhost:8000')
    root_path = os.getenv('ROOT_PATH', '')
    
    # Arguments en ligne de commande
    if len(sys.argv) > 1:
        base_url = sys.argv[1]
    if len(sys.argv) > 2:
        root_path = sys.argv[2]
    
    print("🚀 Test de déploiement SEO Title Analyzer")
    print(f"   Base URL: {base_url}")
    print(f"   Root Path: {root_path}")
    print("=" * 50)
    
    success = test_deployment(base_url, root_path)
    
    if success:
        print("\n✅ Tests réussis ! L'application est correctement déployée.")
        sys.exit(0)
    else:
        print("\n❌ Tests échoués. Vérifiez la configuration.")
        sys.exit(1)

if __name__ == "__main__":
    main() 