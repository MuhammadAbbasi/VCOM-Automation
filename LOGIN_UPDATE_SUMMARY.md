# VCOM Login Automation Update

The VCOM login system has been updated to a modern Keycloak-based SSO (Single Sign-On) flow. I have updated the automation code to handle these changes while continuing to use your existing credentials from `config.json`.

## Key Changes Implementation

### 1. New Login Flow
The authentication now redirects to `auth.meteocontrol.com`. The updated logic in `extraction_code/base_monitor.py` now handles:
- **Dynamic Field Selectors**: Switched to stable IDs used by Keycloak (`#username`, `#password`, `#kc-login`).
- **Multi-Step Support**: Added logic to handle the "Continua" / "Next" buttons that appear in some login sequences.
- **Improved Reliability**: Added localized waiting and better error handling for the login form interactions.

### 2. Navigation Updates
The main dashboard UI has also been refreshed. I updated the navigation logic to accurately find and click the **Valutazione** (Evaluation) section using more robust title-based selectors.

### 3. Verification Results
I performed a live verification using a browser subagent and confirmed:
- **Credentials Valid**: `MarcelloPhoton` successfully authenticated.
- **Redirection Working**: The `SYSTEM_URL` in `config.json` correctly triggers the new login flow.
- **Landing Page confirmed**: The system correctly reaches the "Cockpit Mazara 01" dashboard after login.

---

## Files Updated
- [base_monitor.py](file:///s01/get/2025.01%20Mazara%2001%20A2A/03%20-%20REPORT/Report/09%20Testing/VCOM%20Automation/extraction_code/base_monitor.py): Core login and shared navigation logic.
- [vcom_monitor.py](file:///s01/get/2025.01%20Mazara%2001%20A2A/03%20-%20REPORT/Report/09%20Testing/VCOM%20Automation/vcom_monitor.py): Session health check and orchestrator logic.

---

## Visual Confirmation
The landing page reached after the updated login process:
![VCOM Landing Page](file:///C:/Users/user/.gemini/antigravity/brain/e9271272-def4-4b03-ae75-12493bfed04d/vcom_landing_page_1775729963969.png)
