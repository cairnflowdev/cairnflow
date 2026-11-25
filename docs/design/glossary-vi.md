# Bảng thuật ngữ tiếng Việt — console

Hợp đồng cho `ui/messages/vi.json` (console). Chốt **luật**, không duyệt từng từ. Bảng dưới đây là
luật áp vào các từ đang dùng. Từ mới thì suy theo luật, rồi thêm một dòng vào đây cùng commit.

## Luật

1. **Từ người dùng gõ, hay thấy trong code, API, SDK, CLI, mã lỗi: giữ tiếng Anh.** Viết thường giữa
   câu (`invocation`, `service`), viết hoa khi đứng đầu câu hay làm tên màn.
2. **Còn lại: dịch.** Tên màn không phải tên thực thể, động từ trên nút (trừ thao tác có endpoint riêng — bảng dưới), câu giải thích.
3. **Giá trị trên dây giữ nguyên.** Nhãn hiển thị thì dịch: nhãn trạng thái `failed` là "Thất bại",
   nhưng câu SQL vẫn viết `status = 'failed'`.
4. **key/khoá:**
   - thông tin đăng nhập là "API key";
   - key của object, idempotency key, limit key là "key";
   - "khoá" chỉ mang nghĩa khoá lại (lock của object) hay tắt.
5. **Câu gợi ý tối đa hai câu ngắn.** Chi tiết vào ô "Chi tiết" thu gọn ngay tại chỗ. Khi có trang
   tài liệu cho khách thì thay bằng link.

Máy kiểm:
- console: `ui/src/i18n.test.ts` (mục "thuật ngữ");
- web: `test/web/i18n.test.ts`.

Hai file đỏ khi:
- tên màn lệch bảng;
- "khoá" mang nghĩa key;
- một từ ở cột "không dùng" quay lại;
- một câu gợi ý quá hai câu.

## Giữ tiếng Anh

| Thuật ngữ | Ghi chú |
|---|---|
| invocation | không dịch là "lời gọi" (vẫn được dùng "gọi" làm động từ) |
| service, handler | không dùng "dịch vụ", "bộ xử lý" |
| deployment, deploy | danh từ và động từ (`cairn deploy`); không dùng "triển khai" |
| revision | trường của deployment; "phiên bản" chỉ dùng cho phiên bản cụm/binary |
| workflow, run | handler `run` của workflow: "lượt run" |
| object, virtual object | không dùng "đối tượng" |
| state | state K/V của object. Khác "trạng thái" (status của invocation) |
| journal | không dùng "nhật ký" (nhật ký chỉ dùng cho audit log) |
| awakeable, promise, signal, stream | API của SDK (`ctx.awakeable`, `ctx.promise`, `ctx.signal`, `ctx.stream`); không dùng "lời hứa", "tín hiệu", "luồng" |
| tenant | không dùng "bên thuê" |
| schedule | khi là tên thực thể (màn "Schedules", "tạo schedule"). Lịch cron trong câu thường vẫn là "lịch" |
| rule, scope, limit key | API `/rules` của điều tiết; không dùng "luật" |
| range, keyspace | `sys_ranges`, `cairn-ctl split`; không dùng "dải", "không gian khoá", "khoảng khoá" |
| node, leader, cluster | `cairn-ctl cluster`, vai `cluster-admin`; màn "Cluster", không dùng "cụm" trên console |
| API key | thông tin đăng nhập (`cf_…`) |
| key, idempotency key | không dùng "khoá" cho nghĩa này |
| SQL, trace, span, log | `ctx.trace.span`, `ctx.log` |
| environment | web Cloud: thực thể của cairnflow (`/environments`, console theo environment) |
| serve, invoke | web Cloud, tab quickstart: tên hàm SDK (`serve()`) và lệnh gọi |
| resume, restart, purge, migrate, replay | thao tác có endpoint riêng (`POST /invocations/{id}/resume`…): nút "Resume", "Restart từ bước…", "Đã purge" |

## Dịch

| Tiếng Anh | Tiếng Việt |
|---|---|
| Overview | Tổng quan |
| Archive | Lưu trữ |
| Usage | Mức sử dụng |
| Flow control | Điều tiết |
| Query | Truy vấn |
| Query stats | Thống kê truy vấn |
| Settings | Cài đặt |
| Audit log | Nhật ký audit |
| Objects & state | Objects & state (giữ nguyên, hai tên thực thể) |
| Schedules | Schedules (tên thực thể) |
| limit (giới hạn đồng thời / tốc độ) | giới hạn |
| attempt | lần thử |
| retry (động từ) | thử lại |
| cancel (nút, thao tác) | Huỷ — cùng chữ với nút Huỷ của hộp thoại |
| paused, failed, running… (nhãn trạng thái) | Tạm dừng, Thất bại, Đang chạy… (luật 3) |
| leader of / leads (động từ) | "làm leader", "leader của range" — không dùng "lãnh đạo" |
| sign-in service / identity provider | nhà cung cấp đăng nhập |
| turn | lượt |
| role | vai trò (tên vai giữ nguyên: `view`, `provision`, `admin`) |
| organization (web) | tổ chức — khái niệm chung của SaaS, không có trong SDK hay CLI |
| member, invite (web) | thành viên, lời mời |
| lock (của object) | khoá |
| disable | tắt |
