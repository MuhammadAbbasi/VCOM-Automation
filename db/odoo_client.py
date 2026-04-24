import xmlrpc.client
import logging

logger = logging.getLogger("odoo_client")

class OdooClient:
    def __init__(self, url, db, user, password):
        self.url = url
        self.db = db
        self.user = user
        self.password = password
        self.uid = None
        self.common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
        self.models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")

    def login(self):
        try:
            self.uid = self.common.authenticate(self.db, self.user, self.password, {})
            if not self.uid:
                logger.error("Odoo authentication failed.")
                return False
            return True
        except Exception as e:
            logger.error(f"Odoo connection error: {e}")
            return False

    def create_ticket(self, model, vals):
        if not self.uid and not self.login():
            return None
        
        try:
            ticket_id = self.models.execute_kw(
                self.db, self.uid, self.password,
                model, 'create', [vals]
            )
            return ticket_id
        except Exception as e:
            logger.error(f"Failed to create Odoo ticket: {e}")
            return None

    def update_ticket(self, model, ticket_id, vals):
        if not self.uid and not self.login():
            return False
        
        try:
            self.models.execute_kw(
                self.db, self.uid, self.password,
                model, 'write', [[ticket_id], vals]
            )
            return True
        except Exception as e:
            logger.error(f"Failed to update Odoo ticket {ticket_id}: {e}")
            return False
