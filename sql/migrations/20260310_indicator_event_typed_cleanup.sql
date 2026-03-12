DELETE FROM evt.indicator_event
WHERE indicator_code IN (
    'absorption',
    'initiation',
    'buying_exhaustion',
    'selling_exhaustion'
);
