#!/usr/bin/env ruby
# encoding: utf-8

require './lib/sushi_fabric/sushiApp'

include SushiFabric
describe SushiApp do
  subject(:sushi_app) {SushiApp.new}
  context 'when new' do
    it {is_expected.to be_an_instance_of SushiApp} 
  end 
  describe "#job_header" do
    subject{sushi_app.job_header}
    let(:dataset) {{'Name' => 'Name'}}
    let(:out) {double('out')}
    before do
      allow(out).to receive_messages(:print => nil)
      sushi_app.instance_variable_set(:@out, out)
      sushi_app.instance_variable_set(:@scratch_result_dir, 'scratch_result_dir')
      sushi_app.instance_variable_set(:@dataset, dataset)
    end
    it {is_expected.to be_nil}
  end 
end
