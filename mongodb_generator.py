import pymongo
import json
import random
from datetime import datetime, timedelta
from bson import ObjectId

class MongoDBGenerator:
    def __init__(self, connection_string="mongodb://admin:admin@localhost:27017/"):
        self.client = pymongo.MongoClient(connection_string)
        self.db = self.client["etl_data"]
    
    def generate_sessions(self, count=100):
        sessions = []
        for i in range(count):
            user_id = f"user_{random.randint(1, 50)}"
            start = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 30))
            start = start.replace(hour=random.randint(0, 23))
            
            session = {
                "session_id": f"sess_{i:04d}",
                "user_id": user_id,
                "start_time": start,
                "end_time": start + timedelta(minutes=random.randint(5, 120)),
                "pages_visited": [
                    "/home", 
                    "/products", 
                    f"/products/{random.randint(1, 100)}", 
                    "/cart", 
                    "/checkout"
                ][:random.randint(2, 5)],
                "device": random.choice(["mobile", "desktop", "tablet", "other"]),
                "actions": random.sample(["login", "view_product", "add_to_cart", "purchase", "logout"], random.randint(2, 5))
            }
            sessions.append(session)
        
        self.db.UserSessions.insert_many(sessions)
        return len(sessions)
    
    def generate_event_logs(self, count=200):
        events = []
        event_types = ["click", "scroll", "view", "purchase", "search"]
        for i in range(count):
            timestamp = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 30), hours=random.randint(0, 23))
            event = {
                "event_id": f"evt_{i:04d}",
                "timestamp": timestamp,
                "event_type": random.choice(event_types),
                "details": random.choice(["/home", "/products", "/cart", "/checkout"])
            }
            events.append(event)
        
        self.db.EventLogs.insert_many(events)
        return len(events)
    
    def generate_support_tickets(self, count=50):
        tickets = []
        statuses = ["open", "pending", "closed", "resolved"]
        issues = ["payment", "technical", "shipping", "return", "account"]
        
        for i in range(count):
            created = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 30), hours=random.randint(0, 12))
            updated = created + timedelta(hours=random.randint(1, 48))
            
            ticket = {
                "ticket_id": f"ticket_{i:04d}",
                "user_id": f"user_{random.randint(1, 50)}",
                "status": random.choice(statuses),
                "issue_type": random.choice(issues),
                "messages": [
                    {"sender": "user", "message": "Проблема с заказом", "timestamp": str(created)},
                    {"sender": "support", "message": "Проверим информацию", "timestamp": str(updated)}
                ],
                "created_at": created,
                "updated_at": updated
            }
            tickets.append(ticket)
        
        self.db.SupportTickets.insert_many(tickets)
        return len(tickets)
    
    def generate_recommendations(self, count=30):
        recs = []
        for i in range(count):
            user_id = f"user_{random.randint(1, 50)}"
            products = [f"prod_{random.randint(100, 999)}" for _ in range(random.randint(1, 5))]
            last_updated = datetime(2024, 1, 1) + timedelta(hours=random.randint(0, 168))
            
            rec = {
                "user_id": user_id,
                "recommended_products": products,
                "last_updated": last_updated
            }
            recs.append(rec)
        
        self.db.UserRecommendations.insert_many(recs)
        return len(recs)
    
    def generate_moderation_queue(self, count=80):
        queue = []
        statuses = ["pending", "approved", "rejected", "flagged"]
        
        for i in range(count):
            submitted = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 30), hours=random.randint(0, 23))
            
            review = {
                "review_id": f"rev_{i:04d}",
                "user_id": f"user_{random.randint(1, 50)}",
                "product_id": f"prod_{random.randint(100, 999)}",
                "review_text": random.choice([
                    "Отличный товар, рекомендую!",
                    "Не подошло, верну обратно",
                    "Хорошее качество за свою цену",
                    "Не ожидал такого результата"
                ]),
                "rating": random.randint(1, 5),
                "moderation_status": random.choice(statuses),
                "flags": random.sample(["contains_images", "profanity", "spam"], random.randint(0, 2)),
                "submitted_at": submitted
            }
            queue.append(review)
        
        self.db.ModerationQueue.insert_many(queue)
        return len(queue)
    
    def seed_all(self):
        print("Генерация данных для MongoDB...")
        print(f"  Сессии: {self.generate_sessions(100)}")
        print(f"  События: {self.generate_event_logs(200)}")
        print(f"  Тикеты: {self.generate_support_tickets(50)}")
        print(f"  Рекомендации: {self.generate_recommendations(30)}")
        print(f"  Модерация: {self.generate_moderation_queue(80)}")
        print("Генерация завершена!")

if __name__ == "__main__":
    generator = MongoDBGenerator()
    generator.seed_all()
