from flask import Flask, request, jsonify
import psycopg2
import grpc
from concurrent import futures
import os
import uuid


from grpc_definitions import user_pb2, user_pb2_grpc

app = Flask(__name__)

# ---- CONFIGURAZIONE ----
DB_HOST = os.getenv("DB_HOST", "user_db")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME", "users")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

LISTEN_PORT = int(os.getenv("LISTEN_PORT", 5000))
GRPC_PORT = int(os.getenv("GRPC_PORT", 50051))

# ---- CONNESSIONE AL DATABASE ----
try:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    conn.autocommit = True
    print(f"[UserManager] Connesso a PostgreSQL su {DB_HOST}:{DB_PORT}")
except Exception as e:
    print(f"[UserManager] ERRORE di connessione al DB: {e}")
    conn = None

# ---- FUNZIONI DB ----
def user_exists(email):
    cur = conn.cursor()
    cur.execute("SELECT EXISTS(SELECT 1 FROM users WHERE email=%s)", (email,))
    res = cur.fetchone()[0]
    cur.close()
    return res

def add_user(email, fiscal_code, bank_account):
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO users(email, fiscal_code, bank_account) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING;",
        (email, fiscal_code, bank_account,)
    )
    cur.close()

def delete_user(email):
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM users WHERE email=%s",
        (email,)
    )
    cur.close()

# ---- AT MOST ONE INSTANCE ----
def is_valid_uuid(val):
    try:
        uuid.UUID(str(val))
        return True
    except:
        return False

def already_processed(request_id):
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM processed_requests WHERE request_id=%s", (request_id,))
    exists = cur.fetchone() is not None
    cur.close()
    return exists

def mark_processed(request_id):
    cur = conn.cursor()
    cur.execute("INSERT INTO processed_requests(request_id) VALUES (%s)", (request_id,))
    cur.close()

# ---- ENDPOINT REST ----
@app.route("/register", methods=["POST"])
def register():
    if not request.is_json:
        return jsonify({"error": "Content-Type must be application/json"}), 415

    data = request.get_json()
    email = data.get("email")
    fiscal_code = data.get("fiscal_code")
    bank_account = data.get("bank_account")
    request_id = data.get("request_id")

    if not email:
        return jsonify({"error": "Missing email"}), 400
    if not request_id:
        return jsonify({"error": "Missing request_id"}), 400
    if not is_valid_uuid(request_id):
        return jsonify({"error": "Invalid request_id (must be UUID)"}), 400

    if already_processed(request_id):
        return jsonify({
            "status": "duplicate",
            "message": "Request already processed"
        }), 200

    add_user(email, fiscal_code, bank_account)

    mark_processed(request_id)

    return jsonify({
        "status": "ok",
        "email": email,
        "request_id": request_id
    }), 201

@app.route("/exists/<email>", methods=["GET"])
def exists(email):
    return jsonify({"exists": user_exists(email)})

@app.route("/delete/<email>", methods=["DELETE"])
def delete(email):
    if  user_exists(email):
        delete_user(email)
        return jsonify({"status": "deleted", "email": email})
    else:
        return jsonify({"status": "not found", "email": email}), 404

@app.route("/health", methods=["GET"])
def health():
    status = {"ok": True}
    try:
        if conn is None:
            raise Exception("no db connection")
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        status["db"] = "ok"
    except Exception as e:
        status["ok"] = False
        status["db"] = "fail"
        status["error"] = str(e)
    return jsonify(status), (200 if status["ok"] else 503)

@app.route("/users", methods=["GET"])
def list_users():
    cur = conn.cursor()
    cur.execute("SELECT email FROM users")
    users = [row[0] for row in cur.fetchall()]
    cur.close()
    return jsonify({"users": users})

# ---- SERVIZIO GRPC ----
class UserService(user_pb2_grpc.UserServiceServicer):
    def CheckUserExists(self, request, context):
        print("[gRPC] CheckUserExists chiamato per:", request.email)
        exists = user_exists(request.email)
        return user_pb2.UserCheckResponse(exists=exists)

def start_grpc():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    user_pb2_grpc.add_UserServiceServicer_to_server(UserService(), server)
    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    server.start()
    print(f"[UserManager] gRPC in ascolto su port {GRPC_PORT}")

# ---- MAIN ----
if __name__ == "__main__":
    # Avvia gRPC
    start_grpc()

    # Avvia Flask
    app.run(host="0.0.0.0", port=LISTEN_PORT)
    print(f"[UserManager] REST in ascolto su port {LISTEN_PORT}")
