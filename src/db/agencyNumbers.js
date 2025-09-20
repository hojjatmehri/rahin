import { CONFIG } from "../config/Config.js";

export async function ensureAgencyNumbers() {
  // جدول شماره‌های آژانس
  await CONFIG.db.run(`
    CREATE TABLE IF NOT EXISTS agency_numbers (
      number TEXT PRIMARY KEY
    );
  `);

  await CONFIG.db.exec(`
    DELETE FROM visitor_contacts
    WHERE mobile IN ('989203136002','989203136003','989203136004','989203136005');
  
    DELETE FROM visitor_contacts
    WHERE mobile NOT GLOB '98??????????';
  `);
  

  // شماره واتساپ آژانس
  await CONFIG.db.run(`
    INSERT OR IGNORE INTO agency_numbers(number) VALUES
('989203136002'),
('989203136003'),
('989203136004'),
('989203136005');
  `);

  // جلوگیری از درج شماره‌های آژانس
  await CONFIG.db.run(`
    CREATE TRIGGER IF NOT EXISTS trg_vc_block_agency_numbers
    BEFORE INSERT ON visitor_contacts
    FOR EACH ROW
    WHEN EXISTS (SELECT 1 FROM agency_numbers a WHERE a.number = NEW.mobile)
    BEGIN
      SELECT RAISE(ABORT, 'AGENCY_NUMBER_BLOCKED');
    END;
  `);

  // جلوگیری از آپدیت شماره‌های آژانس
  await CONFIG.db.run(`
    CREATE TRIGGER IF NOT EXISTS trg_vc_block_agency_numbers_upd
    BEFORE UPDATE OF mobile ON visitor_contacts
    FOR EACH ROW
    WHEN EXISTS (SELECT 1 FROM agency_numbers a WHERE a.number = NEW.mobile)
    BEGIN
      SELECT RAISE(ABORT, 'AGENCY_NUMBER_BLOCKED');
    END;
  `);
}
