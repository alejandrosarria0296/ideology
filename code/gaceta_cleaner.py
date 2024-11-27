import pandas as pd
import os
import re
import datetime
import pymupdf
from sklearn.feature_extraction.text import TfidfVectorizer
import nltk
from nltk.corpus import stopwords

nltk.download('stopwords')
span_stopwords = stopwords.words('spanish')

def extract_text_with_bold(filename):
    doc = pymupdf.open(filename)
    extracted_text = ""

    for page in doc:
        blocks = page.get_text("dict")["blocks"]
        for block in blocks:
            if "lines" in block:
                for line in block["lines"]:
                    for span in line["spans"]:
                        text = span["text"]
                        # Check for bold text and annotate it
                        if span["font"].lower().find("bold") >= 0:
                            extracted_text += f"*b-*{text}*-b* "  # Annotate bold text
                        else:
                            extracted_text += text + " "
                    extracted_text += "\n"
            extracted_text += "\n"

    doc.close()
    return extracted_text


def clean_raw_text(text):
    """ 
    Clean the text of a PDF file by removing unnecessary metadata and repeated words. #TODO: detect bold and create "script
    """
    # Remove unnecessary metadata, like headers and footers
    clean_text = re.sub(r"IMPRENTA\s*NACIONAL\s*DE\s*COLOMBIA.*|www\.\w+\.gov\.co", "", text, flags=re.IGNORECASE)
    clean_text = re.sub(r"Página\s*\d+|Edición\s*de.*páginas", "", clean_text, flags=re.IGNORECASE)
    clean_text = re.sub(r"Año.*Nº.*", "", clean_text, flags=re.IGNORECASE)

    # Remove rows that contain only uppercase letters and spaces
    clean_text = "\n".join([line for line in clean_text.splitlines() if re.search(r'[a-z]', line)])
    
    # Remove any extra blank spaces
    clean_text = re.sub(r"\n\s*\n", "\n", clean_text).strip()
    clean_text = re.sub(r"- \n", "", clean_text)

    #join consequitive bold lines
    clean_text = re.sub(r"\*-b\*\s*\n\s*\*b-\*", " ", clean_text)
    clean_text = re.sub(r"\*-b\*", r"\n*-b*\n", clean_text)
    clean_text = re.sub(r"\*b-\*", r"\n*b-*\n", clean_text)
    

    #remove double line breaks
    clean_text = re.sub(r"\n\s*\n", "\n", clean_text).strip()
    clean_text = re.sub(r"[ ]{2,}", " ", clean_text).strip()

    #remove line breaks
    clean_text = re.sub(r"\n", " ", clean_text).strip()

    #esto tiene que ser lo último
    clean_text = re.sub(r"^.*?ACTA NÚMERO \d+ DE \d+", r"", clean_text, count=1, flags=re.DOTALL)[5:]
    return clean_text.strip().lower()


def extract_session_info(raw_text): #TODO: fix this function
    """
    Extracts the session information from the text of a Gaceta del Congreso
    """
    #date
    months_spanish = ["enero", "febrero", "marzo", "abril", "mayo", "junio", 
    "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"]


    date_pattern = r"(\d{1,2})\s+de\s+(" + "|".join(months_spanish) + r")\s+de\s+(\d{4})"
    match = re.search(date_pattern, raw_text, re.IGNORECASE)
    if match:
        date = datetime.date(int(match.group(3)), months_spanish.index(match.group(2)) + 1, int(match.group(1)))
    else:
        date = None

    #chamber and instance
    header = re.sub(r"\s", "", raw_text[:1000]).upper()

    chamber = "house" if "CÁMARADEREPRESENTANTES" in header else "senate"

    instance = "commitee" if "COMISIÓN" in header or "COMISION" in header else "plenary"

    return date, chamber, instance 


def extract_headline_intervention_pairs(text):
    pairs = []
    headline = None
    segments = re.split(r'(\*b-\*.*?\*-b\*)', text, flags=re.DOTALL)

    for segment in segments:
        # Check if the segment is a headline
        if segment.startswith('*b-*') and segment.endswith('*-b*'):
            # Clean up the headline text
            current_headline = segment.replace('*b-*', '').replace('*-b*', '').strip().lower()
            
            # Exclude headlines that contain 'proyecto'
            if 'proyecto' not in current_headline and len(current_headline) > 5:
                if headline is not None:
                    # Append the previous headline and its intervention to the pairs list
                    if len(intervention.strip()) < 5:
                        # Merge short interventions with the previous headline
                        headline += ' ' + current_headline
                    else:
                        pairs.append((headline, intervention.strip()))
                        headline = current_headline
                else:
                    headline = current_headline
                intervention = ""
        else:
            # If it's not a headline, accumulate the text as part of the intervention
            if headline is not None:
                if len(segment.strip()) < 5:
                    # Treat as part of the headline if the intervention is too short
                    headline += ' ' + segment.strip()
                else:
                    intervention += segment.strip() + " "
    
    # Append the last headline and intervention pair
    if headline is not None and intervention:
        pairs.append((headline, intervention.strip()))

    return pairs

def tokenize_intervention(int_pairs, threshold=0.1):
    interventions = [intervention for _, intervention in int_pairs]
    # Initialize vectorizer with Spanish stopwords
    vectorizer = TfidfVectorizer(stop_words=span_stopwords)
    # Fit the vectorizer on the interventions and transform them into a matrix
    tfidf_matrix = vectorizer.fit_transform(interventions)
    # Get the feature names (words)
    feature_names = vectorizer.get_feature_names_out()
    
    # Convert the tf-idf matrix to a dense format and loop over each intervention's row
    tokenized_interventions = []
    dense_matrix = tfidf_matrix.toarray()
    
    for row in dense_matrix:
        # Get words with non-zero tf-idf scores
        words = [feature_names[i] for i in range(len(feature_names)) if row[i] > threshold]
        tokenized_interventions.append(words)
    
    return tokenized_interventions


def process_pdf(file_path): 
    raw_text = extract_text_with_bold(file_path)
    clean_text = clean_raw_text(raw_text)
    info = extract_session_info(clean_text) 
    gaceta_id = os.path.basename(file_path)[:-4]
    intervention_pairs = extract_headline_intervention_pairs(clean_text)
    tokenized_interventions  = tokenize_intervention(intervention_pairs)

    return pd.DataFrame({
        "id": gaceta_id, 
        "date": info[0], 
        "chamber": info[1], 
        "type": info[2], 
        "raw_text": raw_text,
        "clean_text": clean_text,
        "intervention_pairs": [intervention_pairs],
        "interventions": [tokenized_interventions ]},
        index=[0])

if __name__ == "__main__":
    folder = r"D:\Thesis\raw_files"
    destination = r"C:\Users\asarr\Documents\MACSS\Thesis\results\session_data.csv"

    df = pd.DataFrame(columns=["id", "date", "chamber", "type", "raw_text", "clean_text", "intervention_pairs", "interventions"])

    for file_name in os.listdir(folder):
        file_path = os.path.join(folder, file_name)
        try:
            new_row = process_pdf(file_path)
        except Exception as e:
            print(f"error in file {file_name}")
            print(e)
        df = pd.concat([df, new_row], ignore_index=True)

    df.to_csv(destination)