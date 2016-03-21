require 'scala_spec_helper'

describe LuaRowLib do

  let(:globals) { LuaRDD.newGlobals }
  let(:row_values) { [] }
  let(:fields) { [] }
  let(:row) { SqlGenericRow.new(row_values) }
  let(:schema) { SqlStructType.apply(fields) }
  let(:lua_row) { LuaRowValue.new(schema, row) }
  let(:script) { '' }

  before { globals.set('ROW', lua_row) }

  describe '#append' do
    let(:script) { "return row.append(ROW, 'hello', 'world')" }
    let(:result) { globals.load(script).call }

    subject { result }

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

end