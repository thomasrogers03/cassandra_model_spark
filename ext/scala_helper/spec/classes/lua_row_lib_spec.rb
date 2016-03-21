require 'scala_spec_helper'

describe LuaRowLib do

  let(:globals) { LuaRDD.newGlobals }
  let(:row_values) { [] }
  let(:fields) { [] }
  let(:row) { SqlGenericRow.new(row_values) }
  let(:schema) { SqlStructType.apply(fields) }
  let(:lua_row) { LuaRowValue.new(schema, row) }
  let(:script) { '' }
  let(:result) { globals.load(script).call }

  subject { result }

  before { globals.set('ROW', lua_row) }

  describe '#append' do
    let(:script) { "return row.append(ROW, 'hello', 'world')" }

    it { expect(subject.row.apply(0).toString).to eq('world') }
    it { expect(subject.schema.fields[0].name).to eq('hello') }
    it { expect(subject.schema.fields[0].dataType.toString).to eq('StringType') }

    context 'with a different key-value pair to append' do
      let(:script) { "return row.append(ROW, 'goodbye', 55)" }

      it { expect(subject.row.apply(0).toString.to_i).to eq(55) }
      it { expect(subject.schema.fields[0].name).to eq('goodbye') }
      it { expect(subject.schema.fields[0].dataType.toString).to eq('IntegerType') }
    end

    context 'with a double value' do
      let(:script) { "return row.append(ROW, 'bob', 73.7)" }

      it { expect(subject.row.apply(0).toString.to_f).to eq(73.7) }
      it { expect(subject.schema.fields[0].dataType.toString).to eq('DoubleType') }
    end

    context 'with pre-existing data in the row' do
      let(:row_values) { ['some old data'] }
      let(:fields) { [SqlStructField.apply('howdy', SqlStringType, true, nil)] }
      let(:script) { "return row.append(ROW, 'bob', 33.7)" }

      it { expect(subject.row.apply(0).toString).to eq('some old data') }
      it { expect(subject.schema.fields[0].name).to eq('howdy') }
      it { expect(subject.schema.fields[0].dataType.toString).to eq('StringType') }

      it { expect(subject.row.apply(1).toString.to_f).to eq(33.7) }
      it { expect(subject.schema.fields[1].name).to eq('bob') }
      it { expect(subject.schema.fields[1].dataType.toString).to eq('DoubleType') }
    end
  end

  describe '#replace' do
    let(:key) { 'the lost key' }
    let(:row_values) { ['tony bobas'] }
    let(:fields) { [SqlStructField.apply(key, SqlStringType, true, nil)] }
    let(:script) { "return row.replace(ROW, '#{key}', 'johny')" }

    it { expect(subject.row.apply(0).toString).to eq('johny') }

    context 'with a different row' do
      let(:key) { 'the found key' }
      let(:row_values) { ['tony bobas', Faker::Lorem.sentence] }
      let(:fields) do
        [SqlStructField.apply('a different key', SqlStringType, true, nil), SqlStructField.apply(key, SqlStringType, true, nil)]
      end

      it { expect(subject.row.apply(1).toString).to eq('johny') }
    end
  end

  describe '#slice' do
    let(:row_data) { 10.times.inject({}) { |memo, _| memo.merge!(Faker::Lorem.word => Faker::Lorem.sentence) } }
    let(:keys) { row_data.keys }
    let(:slice_keys) { keys.sample(2) }
    let(:row_values) { row_data.values }
    let(:fields) { keys.map { |column| SqlStructField.apply(column, SqlStringType, true, nil) } }
    let(:slice_param) { "{#{slice_keys.map { |column| "'#{column}'" } * ', '}}" }
    let(:script) { "return row.slice(ROW, #{slice_param})" }

    it 'should update the schema with the new keys' do
      expect(subject.schema.fields.map(&:name)).to eq(slice_keys)
    end

    it 'should pull out a slice of values from the row' do
      result_slice = subject.row.toSeq.array.map(&:toString)
      expect(result_slice).to eq(row_data.slice(*slice_keys).values)
    end
  end

end