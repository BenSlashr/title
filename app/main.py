from fastapi import FastAPI, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import csv
import io
import os
import aiofiles
import uuid
import json
import re
import pandas as pd
from datetime import datetime
from .seo_analyzer import analyze_csv, scrape_title, generate_report

app = FastAPI(title="SEO Title Analyzer")

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Templates
templates = Jinja2Templates(directory="templates")

# Create uploads directory if it doesn't exist
os.makedirs("uploads", exist_ok=True)

def parse_semicolon_csv(file_path):
    """
    Parse a CSV file with semicolon separators and possibly with BOM.
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        List of dictionaries with parsed data
    """
    result = []
    
    # Try different encodings
    encodings = ['utf-8-sig', 'utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
    
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                content = f.read()
                
                # Check if the file uses semicolons as separators
                if ';' in content.split('\n')[0]:
                    # Split the content into lines
                    lines = content.strip().split('\n')
                    
                    # Extract header
                    header = lines[0].split(';')
                    header = [h.strip() for h in header]
                    
                    # Process data rows
                    for line in lines[1:]:
                        if not line.strip():
                            continue
                            
                        values = line.split(';')
                        if len(values) >= len(header):
                            row_data = {}
                            for i, col in enumerate(header):
                                if i < len(values):
                                    row_data[col] = values[i].strip()
                            result.append(row_data)
                    
                    if result:
                        return result
        except Exception as e:
            print(f"Failed to read with encoding {encoding}: {str(e)}")
            continue
    
    # If we couldn't parse the file with semicolons, try standard CSV parsing
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                reader = csv.DictReader(f)
                result = list(reader)
                if result:
                    return result
        except Exception as e:
            print(f"Failed to read with standard CSV and encoding {encoding}: {str(e)}")
            continue
    
    raise ValueError("Could not parse the CSV file with any supported format or encoding")

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/upload")
async def upload_file(request: Request, file: UploadFile = File(...)):
    # Save the uploaded file
    file_id = str(uuid.uuid4())
    file_location = f"uploads/{file_id}_{file.filename}"
    
    async with aiofiles.open(file_location, 'wb') as out_file:
        content = await file.read()
        await out_file.write(content)
    
    # Process the file
    try:
        # Parse the CSV file with custom parser
        csv_data = parse_semicolon_csv(file_location)
        
        if not csv_data:
            raise ValueError("Could not read the CSV file or the file is empty")
        
        # Process data
        analysis_results = analyze_csv(csv_data)
        
        # Save results to a session file
        results_file = f"uploads/{file_id}_results.json"
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(analysis_results, f, ensure_ascii=False, indent=2)
        
        return RedirectResponse(url=f"/results?file_id={file_id}", status_code=303)
    except Exception as e:
        return templates.TemplateResponse(
            "error.html", 
            {"request": request, "error": str(e)}
        )

@app.get("/results", response_class=HTMLResponse)
async def results(request: Request, file_id: str):
    # Load results from the session file
    results_file = f"uploads/{file_id}_results.json"
    
    try:
        with open(results_file, 'r', encoding='utf-8') as f:
            results = json.load(f)
        
        return templates.TemplateResponse(
            "results.html", 
            {
                "request": request, 
                "results": results,
                "total_urls": len(results),
                "urls_needing_improvement": sum(1 for r in results if r["optimization_score"] < 70),
                "file_id": file_id  # Pass file_id to the template for export links
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "error.html", 
            {"request": request, "error": str(e)}
        )

@app.get("/export")
async def export_results(file_id: str, format: str = "csv"):
    """
    Export analysis results to CSV or Excel format.
    
    Args:
        file_id: ID of the analysis session
        format: Export format (csv or excel)
        
    Returns:
        Downloadable file in the requested format
    """
    results_file = f"uploads/{file_id}_results.json"
    
    try:
        # Load the results
        with open(results_file, 'r', encoding='utf-8') as f:
            results = json.load(f)
        
        # Create a DataFrame from the results
        df = pd.DataFrame(results)
        
        # Reorder and select columns for better readability
        columns = [
            "url", 
            "original_title", 
            "improved_title", 
            "optimization_score", 
            "title_length", 
            "top_keywords"
        ]
        
        # Select only columns that exist in the DataFrame
        available_columns = [col for col in columns if col in df.columns]
        df = df[available_columns]
        
        # Format the top_keywords column if it exists
        if "top_keywords" in df.columns:
            # Vérifier la structure des données et formater en conséquence
            def format_keywords(keywords):
                if not keywords:
                    return ""
                
                # Si c'est déjà une chaîne, la retourner telle quelle
                if isinstance(keywords, str):
                    return keywords
                
                formatted_keywords = []
                for item in keywords:
                    # Vérifier si l'item est un tuple de 2 éléments (mot-clé, impressions)
                    if isinstance(item, (list, tuple)) and len(item) == 2:
                        kw, imp = item
                        formatted_keywords.append(f"{kw} ({imp})")
                    # Si c'est un dictionnaire avec 'keyword' et 'impressions'
                    elif isinstance(item, dict) and 'keyword' in item and 'impressions' in item:
                        formatted_keywords.append(f"{item['keyword']} ({item['impressions']})")
                    # Si c'est juste une chaîne
                    elif isinstance(item, str):
                        formatted_keywords.append(item)
                    # Sinon, convertir en chaîne
                    else:
                        formatted_keywords.append(str(item))
                
                return ", ".join(formatted_keywords)
            
            df["top_keywords"] = df["top_keywords"].apply(format_keywords)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if format.lower() == "excel":
            # Export to Excel
            output_file = f"uploads/seo_analysis_{timestamp}.xlsx"
            df.to_excel(output_file, index=False, sheet_name="SEO Analysis")
            return FileResponse(
                path=output_file,
                filename=f"seo_analysis_{timestamp}.xlsx",
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            # Default: Export to CSV
            output_file = f"uploads/seo_analysis_{timestamp}.csv"
            df.to_csv(output_file, index=False, encoding="utf-8-sig")  # utf-8-sig for Excel compatibility
            return FileResponse(
                path=output_file,
                filename=f"seo_analysis_{timestamp}.csv",
                media_type="text/csv"
            )
    except Exception as e:
        # Log the error for debugging
        import traceback
        print(f"Export error: {str(e)}")
        print(traceback.format_exc())
        # Return error response
        return {"error": str(e)}
