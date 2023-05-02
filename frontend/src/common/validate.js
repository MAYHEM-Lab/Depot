const TAG_REGEX = /^[a-zA-Z0-9-]+$/
const TAG_LENGTH = 32
const HYPHEN_START = /^-.*$/
const HYPHEN_END = /^.*-$/
const ADJACENT_HYPHEN = /^.*--.*$/

// Returns null if Ok, error else
export default function validateTag(tag) {
    if (!TAG_REGEX.test(tag)) return 'Only letters, digits, and hyphens are allowed'
    if (HYPHEN_START.test(tag)) return 'May not start with a hyphen'
    if (HYPHEN_END.test(tag)) return 'May not end with a hyphen'
    if (ADJACENT_HYPHEN.test(tag)) return 'May not contain adjacent hyphens'
    if (tag.length > TAG_LENGTH) return 'Maximum length is 32 characters'
    return null
}
