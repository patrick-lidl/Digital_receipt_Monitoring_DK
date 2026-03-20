# I. Document Control

|  | Details |
| :--- | :--- |
| **Project Name** | Digital Receipt Monitoring |
| **Git Commit / Version** | 881cbc66 / 1.8.4 |
| **Assessed By** | Kevin Huestis, Christian Heumann |
| **Date** | 2025-12-10 | 

---

# II. PoC-Assessment

**To be included with PoC Codebase**

**Instructions:**

1. Mark your answer in the "Answer" column.
2. If any check in **"In general"** or **"In particular"** is affirmative:
    * 🛑 **Forbidden AI-System**: Hold project immediately & Report to Data & AI Leadership.
3. If any check in **"Additional checks"** is affirmative:
    * ⚠️ **High Risk AI-System**: Risk Management, Governance, & Supervision are obligatory. Contact Domain Lead.

| Category | Question | Answer | Action / Consequence |
| :--- | :--- | :--- | :--- |
| **In general** | Does the system take decisions that could be **potentially harmful to body and life**? | [ ] Yes<br>[x] No | **If YES:**<br>🛑 Forbidden AI-System |
| | Does the system take decisions that could be **potentially harmful to a 3rd party's property or finances**? | [ ] Yes<br>[x] No | |
| | Does the system take decisions that could be **potentially harmful to the environment**? | [ ] Yes<br>[x] No | |
| | Does the system take decisions that could be **potentially harmful to fundamental human rights or applicable law**? | [ ] Yes<br>[x No | |
| | Does the system take decisions that could be **potentially violating Schwarz Group's ethical principles (Code of Conduct)**? | [ ] Yes<br>[x] No | |
| **In particular** | Does the system apply or support **social scoring**? | [ ] Yes<br>[x] No | **If YES:**<br>🛑 Forbidden AI-System |
| | Does the system apply or provide **facial recognition capabilities for general purpose**? | [ ] Yes<br>[x] No | |
| | Does the system **take advantage of (vulnerable) people** with need for protection? | [ ] Yes<br>[x] No | |
| | Does the system apply or support **biometric classification**? | [ ] Yes<br>[x] No | |
| | Does the system take decisions that could be **potentially harmful to the public's or a 3rd party's property or finances**? | [ ] Yes<br>[x] No | |
| | Is the system capable of **predicting, monitoring and tracing crime** in public? | [ ] Yes<br>[x] No | |
| | Is the system capable of **recognizing emotions of employees**? | [ ] Yes<br>[x] No | |
| **Additional checks** | **Is it an AI system intended to be used as a safety component** of a product covered by Union harmonization legislation (Annex I) or is it such a product itself? | [ ] Yes<br>[x] No | **If YES:**<br>⚠️ High Risk AI System<br>Contact Data & AI Domain Lead |
| | **Is it an AI system that falls under one or more of the areas listed in Annex III** of the AI Regulation? | [ ] Yes<br>[x] No | |

<br>

# III. PoV-Assessment

**To be included with PoV Codebase**

| Category | Requirement / Check | Answer | Action (If Requirement Failed) |
| :--- | :--- | :--- | :--- |
| **AI Context** | The business purpose/context of use and the specific task that the AI system will support has been clearly defined. | [x] Yes<br>[ ] No | **IF NO:** Clearly define the business purpose and specific task in project docs. |
| **Data** | Data collection considerations (availability, representativeness, privacy/PII) are identified and documented. | [x] Yes<br>[ ] No | **IF NO:** Document relevant considerations in data/model docs. |
| **Fairness** | The system does not take decisions on discriminatory categories (race, gender, age, religion, disability, sexual orientation). | [x] Yes<br>[ ] No | **IF NO:** Assess bias level (fairness metrics). Define tolerance intervals. Decide on human review. |
| **Explainability** | The system is transparent (non-opaque) in its ability to explain decisions. | [x] Yes<br>[ ] No | **IF NO:** Ensure complete documentation. Decide if decisions require human review. |
| **Security** | Is the data generated through interaction with the “outside world” or accessible from outside the organization? | [ ] Yes<br>[x] No | **IF 2x YES (in this section):** Assess if threats can be accepted, mitigated, or are prohibitive. |
| | Are there entities who could benefit from exploiting potential security risks (competitors, criminals, espionage)? | [ ] Yes<br>[x] No | |
| | Is there a significant impact if the AI system would be corrupted? | [ ] Yes<br>[x] No | |
| **Performance & Monitoring** | System performance is measured qualitatively or quantitatively (accuracy, robustness, privacy, bias, etc.). | [x] Yes<br>[ ] No | **IF 1x NO (in this section):** Mitigate. Track metrics to catch drift. Track model requests. |
| | Appropriateness of data/metrics and effectiveness of controls is regularly assessed (e.g., accounting for drift). | [x] Yes<br>[ ] No | |
| | Model requests are logged/monitored so incidents can be back-tracked. | [x] Yes<br>[ ] No | |

---

# IV. Sign-off

**I confirm that the information provided above is accurate to the best of my knowledge and that the necessary actions regarding High Risk or Forbidden AI systems have been taken.**

<br>

| Role | Name | Signature | Date |
| :--- | :--- | :--- | :--- |
| **Data Scientist** | Kevin Huestis | | 2025-12-10|
| **Reviewer (TL)** | Christian Heumann | | 2025-12-10|
| **Reviewer (BL)** | | | |