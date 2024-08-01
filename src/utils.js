export const formatTime = (timeObj) => {
    if (!timeObj) {
        return "N/A";
    }
    const { days, hours, minutes } = timeObj;
    let formattedTime = [];

    if (days > 0) formattedTime.push(`${days} Day${days > 1 ? 's' : ''}`);
    if (hours > 0) formattedTime.push(`${hours} Hour${hours > 1 ? 's' : ''}`);
    if (minutes > 0) formattedTime.push(`${minutes} Minute${minutes > 1 ? 's' : ''}`);

    return formattedTime.length > 0 ? formattedTime.join(' ') : '0 Minutes';
};
