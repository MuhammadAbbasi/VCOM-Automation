"""
db/odoo_client.py — XML-RPC client for Odoo 18 (fv_* custom modules).

Wraps the fv.sessione.scada / fv.anomalia / fv.intervento workflow.
"""

import logging
import xmlrpc.client
from datetime import datetime

logger = logging.getLogger("odoo_client")

# Plant constants (Mazara 01)
FV_IMPIANTO_ID   = 1   # fv.impianto record ID
PARTNER_ID       = 9   # res.partner "Mazara 01" (used by fv.sessione.scada)
OPERATOR_UID     = 2   # Pietro Artale (admin/operator)


class OdooClient:
    def __init__(self, url: str, db: str, user: str, password: str):
        self.url = url
        self.db = db
        self.user = user
        self.password = password
        self.uid: int | None = None
        self._common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common", allow_none=True)
        self._models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object", allow_none=True)

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    def login(self) -> bool:
        try:
            self.uid = self._common.authenticate(self.db, self.user, self.password, {})
            if not self.uid:
                logger.error("Odoo authentication failed (wrong credentials?)")
                return False
            logger.info(f"Odoo authenticated as uid={self.uid}")
            return True
        except Exception as e:
            logger.error(f"Odoo connection error: {e}")
            return False

    def _rpc(self, model: str, method: str, args=None, kwargs=None):
        if not self.uid:
            if not self.login():
                raise RuntimeError("Odoo not authenticated")
        return self._models.execute_kw(
            self.db, self.uid, self.password,
            model, method, args or [], kwargs or {}
        )

    # ------------------------------------------------------------------
    # SCADA Session  (fv.sessione.scada)
    # ------------------------------------------------------------------

    def _next_seq(self, code: str) -> str:
        """Pull the next value from an Odoo ir.sequence."""
        try:
            return self._rpc("ir.sequence", "next_by_code", [[code]]) or "Auto"
        except Exception:
            return "Auto"

    def create_scada_session(self, fault_summary: str, stato_impianto: str = "alarm") -> int:
        """Create a new SCADA monitoring session and return its ID."""
        vals = {
            "name":             self._next_seq("fv.sessione.scada"),
            "impianto_id":      PARTNER_ID,
            "data_accesso":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "piattaforma":      "custom",
            "piattaforma_custom": "Mazara SCADA",
            "stato_impianto":   stato_impianto,
            "has_tracker":      True,
            "operatore_id":     OPERATOR_UID,
            "note_generali":    fault_summary,
        }
        session_id = self._rpc("fv.sessione.scada", "create", [vals])
        logger.info(f"Created fv.sessione.scada id={session_id}")
        return session_id

    # ------------------------------------------------------------------
    # Anomaly  (fv.anomalia)
    # ------------------------------------------------------------------

    def create_anomalia(
        self,
        session_id: int,
        titolo: str,
        tipo: str,
        priorita: str,
        descrizione: str,
        intervento_richiesto: bool = True,
    ) -> int:
        vals = {
            "sessione_id":         session_id,
            "titolo":              titolo,
            "tipo":                tipo,
            "priorita":            priorita,
            "descrizione":         descrizione,
            "stato":               "aperta",
            "intervento_richiesto": intervento_richiesto,
        }
        anomalia_id = self._rpc("fv.anomalia", "create", [vals])
        logger.info(f"Created fv.anomalia id={anomalia_id} '{titolo}'")
        return anomalia_id

    def resolve_anomalia(self, anomalia_id: int, note: str) -> bool:
        try:
            self._rpc("fv.anomalia", "write", [[anomalia_id], {
                "stato":            "risolta",
                "note_risoluzione": note,
            }])
            return True
        except Exception as e:
            logger.error(f"Failed to resolve anomalia {anomalia_id}: {e}")
            return False

    # ------------------------------------------------------------------
    # Intervention / Work Order  (fv.intervento)
    # ------------------------------------------------------------------

    def create_intervento(
        self,
        titolo: str,
        tipo_intervento: str,
        priorita: str,
        causa_guasto: str,
        session_id: int | None = None,
    ) -> int:
        vals = {
            "name":               self._next_seq("fv.intervento"),
            "impianto_id":        FV_IMPIANTO_ID,
            "data_intervento":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "tipo_intervento":    tipo_intervento,
            "priorita":           priorita,
            "causa_guasto":       causa_guasto,
            "state":              "nuovo",      # awaiting admin assignment
        }
        if session_id:
            vals["sessione_origine_id"] = session_id
        print(f"DEBUG: create_intervento vals={vals}")
        intervento_id = self._rpc("fv.intervento", "create", [vals])
        logger.info(f"Created fv.intervento id={intervento_id} '{titolo}'")
        return intervento_id

    def auto_resolve_intervento(self, intervento_id: int, resolution_note: str) -> bool:
        """Mark as chiuso with an auto-resolution note (SCADA auto-resolved, no admin approval needed)."""
        try:
            self._rpc("fv.intervento", "write", [[intervento_id], {
                "state":               "chiuso",
                "soluzione_adottata":  f"[AUTO RESOLVED] {resolution_note}",
                "lavori_residui":      "Fault cleared automatically by SCADA monitoring.",
            }])
            self._add_chatter(
                "fv.intervento", intervento_id,
                f"✅ Auto-Resolved: {resolution_note}"
            )
            return True
        except Exception as e:
            logger.error(f"Failed to resolve intervento {intervento_id}: {e}")
            return False

    def get_intervento(self, intervento_id: int) -> dict | None:
        try:
            rows = self._rpc("fv.intervento", "read", [[intervento_id]], {
                "fields": ["name", "state", "tecnico_id", "priorita", "tipo_intervento",
                           "data_intervento", "causa_guasto"]
            })
            return rows[0] if rows else None
        except Exception as e:
            logger.error(f"Failed to read intervento {intervento_id}: {e}")
            return None

    # ------------------------------------------------------------------
    # Link anomalia → intervento
    # ------------------------------------------------------------------

    def link_anomalia_to_intervento(self, anomalia_id: int, intervento_id: int):
        self._rpc("fv.anomalia", "write", [[anomalia_id], {"intervento_id": intervento_id}])

    # ------------------------------------------------------------------
    # Chatter  (mail.message)
    # ------------------------------------------------------------------

    def _add_chatter(self, model: str, record_id: int, body: str):
        try:
            self._rpc(model, "message_post", [[record_id]], {
                "body":         body,
                "message_type": "comment",
                "subtype_xmlid": "mail.mt_note",
            })
        except Exception as e:
            logger.debug(f"Chatter post failed (non-critical): {e}")

    # ------------------------------------------------------------------
    # Legacy compatibility
    # ------------------------------------------------------------------

    def create_ticket(self, model: str, vals: dict) -> int | None:
        try:
            return self._rpc(model, "create", [vals])
        except Exception as e:
            logger.error(f"create_ticket failed: {e}")
            return None

    def update_ticket(self, model: str, ticket_id: int, vals: dict) -> bool:
        try:
            self._rpc(model, "write", [[ticket_id], vals])
            return True
        except Exception as e:
            logger.error(f"update_ticket failed: {e}")
            return False
