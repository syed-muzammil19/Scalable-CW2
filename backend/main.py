from fastapi import FastAPI, UploadFile, Form, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions, ContentSettings
from azure.ai.textanalytics import TextAnalyticsClient
from azure.core.credentials import AzureKeyCredential
from datetime import datetime, timedelta
import pyodbc
import os

app = FastAPI()

# ------------------- CORS -------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------- Serve Frontend -------------------
app.mount("/frontend", StaticFiles(directory="frontend", html=True), name="frontend")

# ------------------- Azure Blob -------------------
AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
AZURE_STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")  # for SAS token if needed
CONTAINER_NAME = "videocontainer"

blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)
try:
    container_client.get_container_properties()
except:
    container_client.create_container()

# ------------------- Azure SQL -------------------
SQL_CONNECTION_STRING = (
    f"Driver={{ODBC Driver 17 for SQL Server}};"
    f"Server=tcp:{os.getenv('AZURE_SQL_SERVER')},1433;"
    f"Database={os.getenv('AZURE_SQL_DATABASE')};"
    f"Uid={os.getenv('AZURE_SQL_USERNAME')};"
    f"Pwd={os.getenv('AZURE_SQL_PASSWORD')};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)

try:
    conn = pyodbc.connect(SQL_CONNECTION_STRING)
    cursor = conn.cursor()
    print("Connected to Azure SQL Database successfully.")
except Exception as e:
    print(f"Warning: Could not connect to SQL Database. Comments disabled. {e}")
    conn = None
    cursor = None

# ------------------- Azure Text Analytics -------------------
AZURE_TEXT_ANALYTICS_KEY = os.getenv("AZURE_TEXT_ANALYTICS_KEY")
AZURE_TEXT_ANALYTICS_ENDPOINT = os.getenv("AZURE_TEXT_ANALYTICS_ENDPOINT")

text_analytics_client = TextAnalyticsClient(
    endpoint=AZURE_TEXT_ANALYTICS_ENDPOINT,
    credential=AzureKeyCredential(AZURE_TEXT_ANALYTICS_KEY)
)

def analyze_sentiment(comment: str):
    try:
        response = text_analytics_client.analyze_sentiment([comment])[0]
        return {
            "sentiment": response.sentiment,
            "positive": response.confidence_scores.positive,
            "neutral": response.confidence_scores.neutral,
            "negative": response.confidence_scores.negative
        }
    except Exception as e:
        print(f"Sentiment analysis failed: {e}")
        return {
            "sentiment": "unknown",
            "positive": 0,
            "neutral": 0,
            "negative": 0
        }

# ------------------- API Routes -------------------
@app.post("/upload-video/")
async def upload_video(file: UploadFile):
    try:
        # Force the file to .mp4 if needed
        filename = file.filename
        if not filename.lower().endswith(".mp4"):
            filename = f"{filename.rsplit('.', 1)[0]}.mp4"

        blob_client = container_client.get_blob_client(filename)
        data = await file.read()

        # Upload with fixed Content-Type for browser playback
        blob_client.upload_blob(
            data,
            overwrite=True,
            content_settings=ContentSettings(
                content_type="video/mp4",
                content_disposition="inline"
            )
        )

        # Generate SAS token
        sas_token = generate_blob_sas(
            account_name=os.getenv("AZURE_STORAGE_ACCOUNT_NAME"),
            container_name=CONTAINER_NAME,
            blob_name=filename,
            account_key=AZURE_STORAGE_ACCOUNT_KEY,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(hours=24)
        )

        sas_url = f"{blob_client.url}?{sas_token}"
        return {"video_url": sas_url, "video_name": filename}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/add-comment/")
def add_comment(video_name: str = Form(...), comment_text: str = Form(...)):
    if cursor is None:
        return {"error": "SQL Database not connected."}
    try:
        # Perform sentiment analysis
        response = text_analytics_client.analyze_sentiment([comment_text])[0]
        sentiment = response.sentiment
        pos = response.confidence_scores.positive
        neu = response.confidence_scores.neutral
        neg = response.confidence_scores.negative

        # Insert comment into SQL
        cursor.execute("""
            INSERT INTO Comments (VideoName, CommentText, Sentiment, PositiveScore, NeutralScore, NegativeScore, CreatedAt)
            VALUES (?, ?, ?, ?, ?, ?, GETDATE())
        """, (video_name, comment_text, sentiment, pos, neu, neg))
        conn.commit()

        return {"status": "Comment added", "sentiment": sentiment}

    except Exception as e:
        return {"error": str(e)}

@app.get("/get-comments/")
def get_comments(video_name: str):
    if cursor is None:
        return {"comments": [], "error": "SQL Database not connected."}
    try:
        cursor.execute("""
            SELECT CommentText, Sentiment, PositiveScore, NeutralScore, NegativeScore, CreatedAt
            FROM Comments
            WHERE VideoName = ?
            ORDER BY CreatedAt DESC
        """, (video_name,))
        rows = cursor.fetchall()

        comments = []
        for row in rows:
            comments.append({
                "text": row[0],
                "sentiment": row[1] or "neutral",
                "positive": float(row[2] or 0),
                "neutral": float(row[3] or 0),
                "negative": float(row[4] or 0),
                "created_at": str(row[5])
            })

        return {"comments": comments}

    except Exception as e:
        return {"comments": [], "error": str(e)}
