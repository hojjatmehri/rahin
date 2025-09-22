import axios from 'axios';
const DIDAR_API_KEY = process.env.DIDAR_API_KEY || "gwjwiso8f78l0ohwqx4hm65ft2f9j4zv";
const DIDAR_BASE_URL = process.env.DIDAR_BASE_URL || 'https://app.didar.me/api' ;




export async function findContact(phone) {
    if (!phone) return null;
    try {
        const response = await axios.post(`${DIDAR_BASE_URL}/contact/search?apikey=${DIDAR_API_KEY}`, {
            "Criteria": {
                "MobilePhone": phone
            },
            "From": 0,
            "Limit": 30
        });
        console.log("response.data.Response findContact", JSON.stringify(response.data.Response));
        return response.data.Response?.List[0]?.Id || null;
    } catch (error) {
        console.error(`❌ Error finding contact: ${error.message}`);
        return null;
    }
}

export async function checkDealExists(contactId) {
    console.log("checkDealExists contactId", contactId);
    try {
        // console.log("contactId", contactId);
        if (contactId == null || contactId == undefined || contactId == "")
            return false;
        console.log("🔍 Checking existing deals...");
        const response = await axios.post(`${DIDAR_BASE_URL}/deal/search?apikey=${DIDAR_API_KEY}`, {
            Criteria: {
                ContactIds: [contactId],
            },
            From: 0,
            Limit: 20
        });

        const deals = response.data.Response?.List || [];
        // console.log("*********** deals", JSON.stringify(deals));
        
        return deals;
    } catch (error) {
        console.error("❌ Error checking existing deals in Didar:", error.message);
        return null;
    }
}

export async function findContactName(phone) {
    const contacts = await searchContact({ "MobilePhone": phone });
    return contacts?.List?.length > 0 ? contacts.List[0].DisplayName : null;
}

export async function searchContact(criteria) {
    try {
        const response = await axios.post(`${DIDAR_BASE_URL}/contact/search?apikey=${DIDAR_API_KEY}`, {
            "Criteria": criteria,
            "From": 0,
            "Limit": 100
        });
        return response.data.Response || null;
    } catch (error) {
        console.error('❌ Error searching contact:', error.message);
        return null;
    }
}


// ------------------------ دریافت Pipelineها ------------------------
export async function getPipelines() {
    try {
        const response = await axios.post(`${DIDAR_BASE_URL}/pipeline/list/0?apikey=${DIDAR_API_KEY}`);
        return response.data.Response || [];
    } catch (error) {
        console.error('❌ Error fetching pipelines:', error.message);
        return [];
    }
}

export async function searchDealsByRegisterRange(fromISO, toISO, from = 0, limit = 100) {
    // console.log(`Searching deals from ${fromISO} to ${toISO}, offset ${from}, limit ${limit}`);
    try {
        const payload = {
            Criteria: {
                SearchFromTime: fromISO,
                SearchToTime: toISO,
            },
            From: from,
            Limit: limit,
        };

        const { data } = await axios.post(
            `${DIDAR_BASE_URL}/deal/search?apikey=${DIDAR_API_KEY}`,
            payload,
            { headers: { "Content-Type": "application/json" }, timeout: 25000 }
        );

        const list = data?.Response?.List || [];

        // تاریخ و ساعت یکتا برای نام فایل
        const now = new Date();
        const timestamp = now
            .toISOString()
            .replace(/[-:]/g, "")   // حذف خط تیره و دو نقطه
            .replace(/\..+/, "");   // حذف میلی‌ثانیه
        // خروجی مثلاً: 20250907T123456

        // const fileName = `searchDeals_${timestamp}.txt`;
        // const filePath = path.join(process.cwd(), fileName);

        // // ذخیره در فایل
        // fs.writeFileSync(
        //     filePath,
        //     `from=${fromISO} to=${toISO}\n${JSON.stringify(list, null, 2)}\n`
        // );

        return list;
    } catch (error) {
        console.error("❌ Error searchDealsByRegisterRange:", error.response?.data || error.message);
        return [];
    }
}

export async function getDealDetailById(dealId) {
    console.log("getDealDetailById dealId", dealId);
    if (!dealId) return null;
    try {
        const { data } = await axios.post(
            `${DIDAR_BASE_URL}/deal/GetDealDetail?apikey=${DIDAR_API_KEY}`,
            { Id: dealId },
            { headers: { "Content-Type": "application/json" }, timeout: 25000 }
        );
        console.log("*********** getDealDetailById", JSON.stringify(data?.Response));
        return data?.Response || null;
    } catch (error) {
        console.error("❌ Error getDealDetailById:", error.response?.data || error.message);
        return null;
    }
}
