# KPI-Leitfaden

Für Werksleitung, Supply Chain und Kundendienst. Pro Kennzahl: was sie misst, wie sie berechnet wird, wie man sie liest und was zu tun ist, wenn sie sich verändert.

| KPI | Definition | Formel | Lesart | Bei Verschlechterung zuerst fragen |
|---|---|---|---|---|
| **OTIF %** | Anteil gelieferter Auftragspositionen, die pünktlich *und* vollständig geliefert wurden | OTIF-Positionen ÷ gelieferte Positionen | Zuverlässigkeit aus Kundensicht. Eine verspätete oder unvollständige Position zählt als Fehler | Welches Werk, welches Segment? Verspätet (Kapazität, Material) oder unvollständig (Bestand)? |
| **Liefertreue %** | Anteil der Positionen, die bis zum Wunschtermin versandt wurden | pünktliche ÷ gelieferte Positionen | Differenz zu OTIF = Mengenprobleme | Termintreue der Fertigung und Lieferantentermintreue im selben Zeitraum prüfen |
| **Ausschussquote %** | Ausschuss im Verhältnis zur Gesamtmenge | Ausschuss ÷ (Gutmenge + Ausschuss) | Qualitätskosten des Prozesses. Werke und Arbeitsplätze vergleichen, nicht einzelne Aufträge | Welcher Arbeitsplatz? Neues Material, neue Mitarbeitende, Maschinenwechsel? |
| **Ausschusskosten CHF** | Ausschuss zu Standardkosten | Σ Ausschussmenge × Standardkosten | Macht Ausschuss in Franken sichtbar. Danach priorisieren, nicht nach der Quote | Top 10 Materialien nach Ausschusskosten |
| **Planerfüllung %** | Gutmenge im Verhältnis zur Planmenge abgeschlossener Aufträge | Gutmenge ÷ Planmenge | Unter 100 % ging geplante Menge verloren | Ausschuss oder vorzeitig abgeschlossene Aufträge? |
| **Termintreue Fertigung %** | Abgeschlossene Aufträge, die bis zum Planende fertig waren | termintreue ÷ abgeschlossene Aufträge | Wirkt zwei bis vier Wochen später auf OTIF | Engpass-Arbeitsplatz? Material zu spät? |
| **Bestandswert CHF** | Bestand zum letzten Stichtag zu Standardkosten | Σ Bestand × Standardkosten | Gebundenes Kapital. Auf den Verlauf achten, nicht auf den Stand | Welche Materialart ist gewachsen? Langsamdreher? |
| **Positionen unter Sicherheitsbestand** | Werk/Material-Kombinationen unter Sicherheitsbestand am letzten Stichtag | Anzahl | Fehlteilrisiko für Fertigung und Lieferung | Offene Bestellungen und ihre bestätigten Termine |
| **Reichweite (Tage)** | Bestand ÷ durchschnittlicher Tagesbedarf (Verkauf + über Stückliste aufgelöster Fertigungsbedarf) | Bestand ÷ Ø Tagesbedarf | Unter Wiederbeschaffungszeit = Risiko. Weit darüber = Überbestand | Mit der Wiederbeschaffungszeit vergleichen |
| **Lieferantentermintreue %** | Wareneingänge bis zum bestätigten Termin | pünktliche ÷ alle Wareneingänge | Zuverlässigkeit der Lieferanten | Welche Lieferanten? Mit Daten eskalieren |
| **Datenqualität %** | Anteil der Zeilen, die im letzten Lauf alle Regeln erfüllen | 1 − fehlerhafte ÷ geprüfte Zeilen | Vertrauensniveau aller anderen Zahlen | Seite Datenqualität öffnen: welche Regel, welche Tabelle? An der Quelle beheben |

## Seite Datenqualität lesen
- **Error**-Regeln nehmen Zeilen aus dem Reporting und legen sie in Quarantäne. Die Kennzahlen oben enthalten diese Zeilen nicht.
- **Warning**-Regeln behalten die Zeile und markieren sie nur.
- Steigt eine Regel an, deutet das meist auf einen Prozess oder eine Schnittstelle hin. Verantwortliche Person benennen, im ERP beheben und im nächsten Lauf kontrollieren, dass der Wert sinkt.
