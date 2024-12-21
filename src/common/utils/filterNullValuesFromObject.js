const disallowedValues = [
  '[not provided]',
  'placeholder',
  '[[unknown]]',
  'not set',
  'not provided',
  'unknown',
  'undefined',
  'n/a',
  '',
  null,
  undefined
];

export const filterNullValuesFromObject = object => {
  return Object.fromEntries(
    Object.entries(object).filter(([_, value]) => {
      const isAllowed = !disallowedValues.includes(value.toLowerCase)
      const hasNoRecordString = !value.toLowerCase().includes('!$record')
      return isAllowed && hasNoRecordString
    })
  );
}