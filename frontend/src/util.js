import moment from "moment";
import 'moment-duration-format'

const TIME_UNITS = {
    'nanosecond': 0.000001,
    'microsecond': 0.001,
    'millisecond': 1,
    'second': 1000,
    'minute': 60 * 1000,
    'hour': 3600 * 1000,
    'day': 24 * 3600 * 1000
}

const util = {
    isNumeric: (val) => {
        const intRegex = /^\d+?$/
        const num = parseInt(val)
        return intRegex.test(val) && !isNaN(num) && num > 0
    },

    parseDuration: (durationStr) => {
        const millis = durationStr.split('+').reduce((sum, part) => {
            const [duration, unit] = part.split('.')
            const scale = TIME_UNITS[unit.toLowerCase().replace(/s$/, '')]
            return sum + duration * scale
        }, 0)
       return moment.duration(millis, 'milliseconds')
    },

    formatDuration: (duration) => {
        return duration.format('d [days], h [hours], m [minutes]', {trim: 'all'})
    },

    formatTime: (epochMs) => {
        return moment(epochMs).format("YYYY-MM-DD h:mm A")
    },

    formatBytes: (bytes) => {
        if (bytes === 0) return '0 bytes';

        const k = 1000;
        const sizes = ['bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];

        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }
}

export default util