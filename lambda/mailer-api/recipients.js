// Recipient hygiene for the mailer gateway. Demo users type junk addresses
// (test@test.com, foo@bar) that either fail at SES send time or hard-bounce
// and eat into the account's SES bounce rate. Fake recipients are silently
// dropped before enqueue; an email whose recipients are all fake is
// acknowledged with FAKE_EMAIL_ID and never sent. GET on that sentinel id
// returns a synthetic "delivered" response so polling clients work unchanged.

// Sentinel id returned instead of a real emailId when nothing was sent.
const FAKE_EMAIL_ID = "00000000-0000-0000-0000-000000000000";

// Pragmatic syntax check, deliberately stricter than RFC 5322: one @, dotted
// local part without empty labels, domain labels without leading/trailing
// hyphens, and an alphabetic TLD of 2+ chars. Anything it rejects is not
// worth risking a bounce for.
const SYNTAX =
  /^[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+(\.[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+)*@[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?(\.[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?)*\.[A-Za-z]{2,}$/;

// Reserved TLDs that can never receive mail (RFC 2606 / RFC 6761).
const BLOCKED_TLDS = new Set(["test", "invalid", "localhost", "local", "example", "internal", "demo"]);

// Reserved documentation domains plus well-known disposable-inbox providers.
// Matches subdomains too (anything@sub.mailinator.com is just as fake).
const BLOCKED_DOMAINS = new Set([
  "example.com",
  "example.org",
  "example.net",
  "example.edu",
  "test.com",
  "mailinator.com",
  "yopmail.com",
  "guerrillamail.com",
  "sharklasers.com",
  "grr.la",
  "temp-mail.org",
  "tempmail.com",
  "tempail.com",
  "10minutemail.com",
  "20minutemail.com",
  "throwawaymail.com",
  "getnada.com",
  "maildrop.cc",
  "dispostable.com",
  "trashmail.com",
  "trash-mail.com",
  "fakeinbox.com",
  "mailnesia.com",
  "mintemail.com",
  "mohmal.com",
  "emailondeck.com",
  "mytemp.email",
  "burnermail.io",
  "discard.email",
  "spamgourmet.com",
]);

// Accepts "user@domain" or "Display Name <user@domain>"; returns the bare address.
function extractAddress(value) {
  const m = /<([^<>]+)>\s*$/.exec(value);
  return (m ? m[1] : value).trim();
}

function hasValidSyntax(value) {
  return SYNTAX.test(extractAddress(String(value)));
}

// Deliverability gate for recipients: valid syntax plus a domain that is
// neither reserved nor a disposable-inbox provider (including subdomains).
function isDeliverable(value) {
  const address = extractAddress(String(value));
  if (!SYNTAX.test(address)) return false;
  const domain = address.slice(address.lastIndexOf("@") + 1).toLowerCase();
  if (BLOCKED_TLDS.has(domain.slice(domain.lastIndexOf(".") + 1))) return false;
  const labels = domain.split(".");
  for (let i = 0; i < labels.length - 1; i++) {
    if (BLOCKED_DOMAINS.has(labels.slice(i).join("."))) return false;
  }
  return true;
}

module.exports = { FAKE_EMAIL_ID, hasValidSyntax, isDeliverable };
