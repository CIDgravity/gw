package boosttypes

import (
	"fmt"
	"io"
	"math"
	"sort"

	abi "github.com/filecoin-project/go-state-types/abi"
	cid "github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
	xerrors "golang.org/x/xerrors"
)

var _ = xerrors.Errorf
var _ = cid.Undef
var _ = math.E
var _ = sort.Sort

func (t *StorageAsk) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}

	cw := cbg.NewCborWriter(w)

	if _, err := cw.Write([]byte{165}); err != nil {
		return err
	}

	// t.Price (big.Int) (struct)
	if len("Price") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"Price\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("Price"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("Price")); err != nil {
		return err
	}

	if err := t.Price.MarshalCBOR(cw); err != nil {
		return err
	}

	// t.VerifiedPrice (big.Int) (struct)
	if len("VerifiedPrice") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"VerifiedPrice\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("VerifiedPrice"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("VerifiedPrice")); err != nil {
		return err
	}

	if err := t.VerifiedPrice.MarshalCBOR(cw); err != nil {
		return err
	}

	// t.MinPieceSize (abi.PaddedPieceSize) (uint64)
	if len("MinPieceSize") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"MinPieceSize\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("MinPieceSize"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("MinPieceSize")); err != nil {
		return err
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajUnsignedInt, uint64(t.MinPieceSize)); err != nil {
		return err
	}

	// t.MaxPieceSize (abi.PaddedPieceSize) (uint64)
	if len("MaxPieceSize") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"MaxPieceSize\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("MaxPieceSize"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("MaxPieceSize")); err != nil {
		return err
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajUnsignedInt, uint64(t.MaxPieceSize)); err != nil {
		return err
	}

	// t.Miner (address.Address) (struct)
	if len("Miner") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"Miner\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("Miner"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("Miner")); err != nil {
		return err
	}

	if err := t.Miner.MarshalCBOR(cw); err != nil {
		return err
	}
	return nil
}

func (t *StorageAsk) UnmarshalCBOR(r io.Reader) (err error) {
	*t = StorageAsk{}

	cr := cbg.NewCborReader(r)

	maj, extra, err := cr.ReadHeader()
	if err != nil {
		return err
	}
	defer func() {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
	}()

	if maj != cbg.MajMap {
		return fmt.Errorf("cbor input should be of type map")
	}

	if extra > cbg.MaxLength {
		return fmt.Errorf("StorageAsk: map struct too large (%d)", extra)
	}

	var name string
	n := extra

	for i := uint64(0); i < n; i++ {

		{
			sval, err := cbg.ReadString(cr)
			if err != nil {
				return err
			}

			name = string(sval)
		}

		switch name {
		// t.Price (big.Int) (struct)
		case "Price":

			{

				if err := t.Price.UnmarshalCBOR(cr); err != nil {
					return xerrors.Errorf("unmarshaling t.Price: %w", err)
				}

			}
			// t.VerifiedPrice (big.Int) (struct)
		case "VerifiedPrice":

			{

				if err := t.VerifiedPrice.UnmarshalCBOR(cr); err != nil {
					return xerrors.Errorf("unmarshaling t.VerifiedPrice: %w", err)
				}

			}
			// t.MinPieceSize (abi.PaddedPieceSize) (uint64)
		case "MinPieceSize":

			{

				maj, extra, err = cr.ReadHeader()
				if err != nil {
					return err
				}
				if maj != cbg.MajUnsignedInt {
					return fmt.Errorf("wrong type for uint64 field")
				}
				t.MinPieceSize = abi.PaddedPieceSize(extra)

			}
			// t.MaxPieceSize (abi.PaddedPieceSize) (uint64)
		case "MaxPieceSize":

			{

				maj, extra, err = cr.ReadHeader()
				if err != nil {
					return err
				}
				if maj != cbg.MajUnsignedInt {
					return fmt.Errorf("wrong type for uint64 field")
				}
				t.MaxPieceSize = abi.PaddedPieceSize(extra)

			}
			// t.Miner (address.Address) (struct)
		case "Miner":

			{

				if err := t.Miner.UnmarshalCBOR(cr); err != nil {
					return xerrors.Errorf("unmarshaling t.Miner: %w", err)
				}

			}

		default:
			// Field doesn't exist on this type, so ignore it
			cbg.ScanForLinks(r, func(cid.Cid) {})
		}
	}

	return nil
}
func (t *DealParams) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}

	cw := cbg.NewCborWriter(w)

	if _, err := cw.Write([]byte{167}); err != nil {
		return err
	}

	// t.DealUUID (uuid.UUID) (array)
	if len("DealUUID") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"DealUUID\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("DealUUID"))); err != nil {
		return err
	}
	if _, err := cw.WriteString(string("DealUUID")); err != nil {
		return err
	}

	if len(t.DealUUID) > cbg.ByteArrayMaxLen {
		return xerrors.Errorf("Byte array in field t.DealUUID was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajByteString, uint64(len(t.DealUUID))); err != nil {
		return err
	}

	if _, err := cw.Write(t.DealUUID[:]); err != nil {
		return err
	}

	// t.Transfer (types.Transfer) (struct)
	if len("Transfer") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"Transfer\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("Transfer"))); err != nil {
		return err
	}
	if _, err := cw.WriteString(string("Transfer")); err != nil {
		return err
	}

	if err := t.Transfer.MarshalCBOR(cw); err != nil {
		return err
	}

	// t.IsOffline (bool) (bool)
	if len("IsOffline") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"IsOffline\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("IsOffline"))); err != nil {
		return err
	}
	if _, err := cw.WriteString(string("IsOffline")); err != nil {
		return err
	}

	if err := cbg.WriteBool(w, t.IsOffline); err != nil {
		return err
	}

	// t.DealDataRoot (cid.Cid) (struct)
	if len("DealDataRoot") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"DealDataRoot\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("DealDataRoot"))); err != nil {
		return err
	}
	if _, err := cw.WriteString(string("DealDataRoot")); err != nil {
		return err
	}

	if err := cbg.WriteCid(cw, t.DealDataRoot); err != nil {
		return xerrors.Errorf("failed to write cid field t.DealDataRoot: %w", err)
	}

	// t.SkipIPNIAnnounce (bool) (bool)
	if len("SkipIPNIAnnounce") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"SkipIPNIAnnounce\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("SkipIPNIAnnounce"))); err != nil {
		return err
	}
	if _, err := cw.WriteString(string("SkipIPNIAnnounce")); err != nil {
		return err
	}

	if err := cbg.WriteBool(w, t.SkipIPNIAnnounce); err != nil {
		return err
	}

	// t.ClientDealProposal (market.ClientDealProposal) (struct)
	if len("ClientDealProposal") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"ClientDealProposal\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("ClientDealProposal"))); err != nil {
		return err
	}
	if _, err := cw.WriteString(string("ClientDealProposal")); err != nil {
		return err
	}

	if err := t.ClientDealProposal.MarshalCBOR(cw); err != nil {
		return err
	}

	// t.RemoveUnsealedCopy (bool) (bool)
	if len("RemoveUnsealedCopy") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"RemoveUnsealedCopy\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("RemoveUnsealedCopy"))); err != nil {
		return err
	}
	if _, err := cw.WriteString(string("RemoveUnsealedCopy")); err != nil {
		return err
	}

	if err := cbg.WriteBool(w, t.RemoveUnsealedCopy); err != nil {
		return err
	}
	return nil
}

func (t *DealParams) UnmarshalCBOR(r io.Reader) (err error) {
	*t = DealParams{}

	cr := cbg.NewCborReader(r)

	maj, extra, err := cr.ReadHeader()
	if err != nil {
		return err
	}
	defer func() {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
	}()

	if maj != cbg.MajMap {
		return fmt.Errorf("cbor input should be of type map")
	}

	if extra > cbg.MaxLength {
		return fmt.Errorf("DealParams: map struct too large (%d)", extra)
	}

	var name string
	n := extra

	for i := uint64(0); i < n; i++ {

		{
			sval, err := cbg.ReadString(cr)
			if err != nil {
				return err
			}

			name = string(sval)
		}

		switch name {
		// t.DealUUID (uuid.UUID) (array)
		case "DealUUID":

			maj, extra, err = cr.ReadHeader()
			if err != nil {
				return err
			}

			if extra > cbg.ByteArrayMaxLen {
				return fmt.Errorf("t.DealUUID: byte array too large (%d)", extra)
			}
			if maj != cbg.MajByteString {
				return fmt.Errorf("expected byte array")
			}

			if extra != 16 {
				return fmt.Errorf("expected array to have 16 elements")
			}

			t.DealUUID = [16]uint8{}

			if _, err := io.ReadFull(cr, t.DealUUID[:]); err != nil {
				return err
			}
			// t.Transfer (types.Transfer) (struct)
		case "Transfer":

			{

				if err := t.Transfer.UnmarshalCBOR(cr); err != nil {
					return xerrors.Errorf("unmarshaling t.Transfer: %w", err)
				}

			}
			// t.IsOffline (bool) (bool)
		case "IsOffline":

			maj, extra, err = cr.ReadHeader()
			if err != nil {
				return err
			}
			if maj != cbg.MajOther {
				return fmt.Errorf("booleans must be major type 7")
			}
			switch extra {
			case 20:
				t.IsOffline = false
			case 21:
				t.IsOffline = true
			default:
				return fmt.Errorf("booleans are either major type 7, value 20 or 21 (got %d)", extra)
			}
			// t.DealDataRoot (cid.Cid) (struct)
		case "DealDataRoot":

			{

				c, err := cbg.ReadCid(cr)
				if err != nil {
					return xerrors.Errorf("failed to read cid field t.DealDataRoot: %w", err)
				}

				t.DealDataRoot = c

			}
			// t.SkipIPNIAnnounce (bool) (bool)
		case "SkipIPNIAnnounce":

			maj, extra, err = cr.ReadHeader()
			if err != nil {
				return err
			}
			if maj != cbg.MajOther {
				return fmt.Errorf("booleans must be major type 7")
			}
			switch extra {
			case 20:
				t.SkipIPNIAnnounce = false
			case 21:
				t.SkipIPNIAnnounce = true
			default:
				return fmt.Errorf("booleans are either major type 7, value 20 or 21 (got %d)", extra)
			}
			// t.ClientDealProposal (market.ClientDealProposal) (struct)
		case "ClientDealProposal":

			{

				if err := t.ClientDealProposal.UnmarshalCBOR(cr); err != nil {
					return xerrors.Errorf("unmarshaling t.ClientDealProposal: %w", err)
				}

			}
			// t.RemoveUnsealedCopy (bool) (bool)
		case "RemoveUnsealedCopy":

			maj, extra, err = cr.ReadHeader()
			if err != nil {
				return err
			}
			if maj != cbg.MajOther {
				return fmt.Errorf("booleans must be major type 7")
			}
			switch extra {
			case 20:
				t.RemoveUnsealedCopy = false
			case 21:
				t.RemoveUnsealedCopy = true
			default:
				return fmt.Errorf("booleans are either major type 7, value 20 or 21 (got %d)", extra)
			}

		default:
			// Field doesn't exist on this type, so ignore it
			cbg.ScanForLinks(r, func(cid.Cid) {})
		}
	}

	return nil
}
func (t *Transfer) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}

	cw := cbg.NewCborWriter(w)

	if _, err := cw.Write([]byte{164}); err != nil {
		return err
	}

	// t.Type (string) (string)
	if len("Type") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"Type\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("Type"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("Type")); err != nil {
		return err
	}

	if len(t.Type) > cbg.MaxLength {
		return xerrors.Errorf("Value in field t.Type was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len(t.Type))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string(t.Type)); err != nil {
		return err
	}

	// t.ClientID (string) (string)
	if len("ClientID") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"ClientID\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("ClientID"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("ClientID")); err != nil {
		return err
	}

	if len(t.ClientID) > cbg.MaxLength {
		return xerrors.Errorf("Value in field t.ClientID was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len(t.ClientID))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string(t.ClientID)); err != nil {
		return err
	}

	// t.Params ([]uint8) (slice)
	if len("Params") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"Params\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("Params"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("Params")); err != nil {
		return err
	}

	if len(t.Params) > cbg.ByteArrayMaxLen {
		return xerrors.Errorf("Byte array in field t.Params was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajByteString, uint64(len(t.Params))); err != nil {
		return err
	}

	if _, err := cw.Write(t.Params[:]); err != nil {
		return err
	}

	// t.Size (uint64) (uint64)
	if len("Size") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"Size\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("Size"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("Size")); err != nil {
		return err
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajUnsignedInt, uint64(t.Size)); err != nil {
		return err
	}

	return nil
}

func (t *Transfer) UnmarshalCBOR(r io.Reader) (err error) {
	*t = Transfer{}

	cr := cbg.NewCborReader(r)

	maj, extra, err := cr.ReadHeader()
	if err != nil {
		return err
	}
	defer func() {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
	}()

	if maj != cbg.MajMap {
		return fmt.Errorf("cbor input should be of type map")
	}

	if extra > cbg.MaxLength {
		return fmt.Errorf("Transfer: map struct too large (%d)", extra)
	}

	var name string
	n := extra

	for i := uint64(0); i < n; i++ {

		{
			sval, err := cbg.ReadString(cr)
			if err != nil {
				return err
			}

			name = string(sval)
		}

		switch name {
		// t.Type (string) (string)
		case "Type":

			{
				sval, err := cbg.ReadString(cr)
				if err != nil {
					return err
				}

				t.Type = string(sval)
			}
			// t.ClientID (string) (string)
		case "ClientID":

			{
				sval, err := cbg.ReadString(cr)
				if err != nil {
					return err
				}

				t.ClientID = string(sval)
			}
			// t.Params ([]uint8) (slice)
		case "Params":

			maj, extra, err = cr.ReadHeader()
			if err != nil {
				return err
			}

			if extra > cbg.ByteArrayMaxLen {
				return fmt.Errorf("t.Params: byte array too large (%d)", extra)
			}
			if maj != cbg.MajByteString {
				return fmt.Errorf("expected byte array")
			}

			if extra > 0 {
				t.Params = make([]uint8, extra)
			}

			if _, err := io.ReadFull(cr, t.Params[:]); err != nil {
				return err
			}
			// t.Size (uint64) (uint64)
		case "Size":

			{

				maj, extra, err = cr.ReadHeader()
				if err != nil {
					return err
				}
				if maj != cbg.MajUnsignedInt {
					return fmt.Errorf("wrong type for uint64 field")
				}
				t.Size = uint64(extra)

			}

		default:
			// Field doesn't exist on this type, so ignore it
			cbg.ScanForLinks(r, func(cid.Cid) {})
		}
	}

	return nil
}
func (t *DealResponse) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}

	cw := cbg.NewCborWriter(w)

	if _, err := cw.Write([]byte{162}); err != nil {
		return err
	}

	// t.Accepted (bool) (bool)
	if len("Accepted") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"Accepted\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("Accepted"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("Accepted")); err != nil {
		return err
	}

	if err := cbg.WriteBool(w, t.Accepted); err != nil {
		return err
	}

	// t.Message (string) (string)
	if len("Message") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"Message\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("Message"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("Message")); err != nil {
		return err
	}

	if len(t.Message) > cbg.MaxLength {
		return xerrors.Errorf("Value in field t.Message was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len(t.Message))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string(t.Message)); err != nil {
		return err
	}
	return nil
}

func (t *DealResponse) UnmarshalCBOR(r io.Reader) (err error) {
	*t = DealResponse{}

	cr := cbg.NewCborReader(r)

	maj, extra, err := cr.ReadHeader()
	if err != nil {
		return err
	}
	defer func() {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
	}()

	if maj != cbg.MajMap {
		return fmt.Errorf("cbor input should be of type map")
	}

	if extra > cbg.MaxLength {
		return fmt.Errorf("DealResponse: map struct too large (%d)", extra)
	}

	var name string
	n := extra

	for i := uint64(0); i < n; i++ {

		{
			sval, err := cbg.ReadString(cr)
			if err != nil {
				return err
			}

			name = string(sval)
		}

		switch name {
		// t.Accepted (bool) (bool)
		case "Accepted":

			maj, extra, err = cr.ReadHeader()
			if err != nil {
				return err
			}
			if maj != cbg.MajOther {
				return fmt.Errorf("booleans must be major type 7")
			}
			switch extra {
			case 20:
				t.Accepted = false
			case 21:
				t.Accepted = true
			default:
				return fmt.Errorf("booleans are either major type 7, value 20 or 21 (got %d)", extra)
			}
			// t.Message (string) (string)
		case "Message":

			{
				sval, err := cbg.ReadString(cr)
				if err != nil {
					return err
				}

				t.Message = string(sval)
			}

		default:
			// Field doesn't exist on this type, so ignore it
			cbg.ScanForLinks(r, func(cid.Cid) {})
		}
	}

	return nil
}
func (t *DealStatusRequest) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}

	cw := cbg.NewCborWriter(w)

	if _, err := cw.Write([]byte{162}); err != nil {
		return err
	}

	// t.DealUUID (uuid.UUID) (array)
	if len("DealUUID") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"DealUUID\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("DealUUID"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("DealUUID")); err != nil {
		return err
	}

	if len(t.DealUUID) > cbg.ByteArrayMaxLen {
		return xerrors.Errorf("Byte array in field t.DealUUID was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajByteString, uint64(len(t.DealUUID))); err != nil {
		return err
	}

	if _, err := cw.Write(t.DealUUID[:]); err != nil {
		return err
	}

	// t.Signature (crypto.Signature) (struct)
	if len("Signature") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"Signature\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("Signature"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("Signature")); err != nil {
		return err
	}

	if err := t.Signature.MarshalCBOR(cw); err != nil {
		return err
	}
	return nil
}

func (t *DealStatusRequest) UnmarshalCBOR(r io.Reader) (err error) {
	*t = DealStatusRequest{}

	cr := cbg.NewCborReader(r)

	maj, extra, err := cr.ReadHeader()
	if err != nil {
		return err
	}
	defer func() {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
	}()

	if maj != cbg.MajMap {
		return fmt.Errorf("cbor input should be of type map")
	}

	if extra > cbg.MaxLength {
		return fmt.Errorf("DealStatusRequest: map struct too large (%d)", extra)
	}

	var name string
	n := extra

	for i := uint64(0); i < n; i++ {

		{
			sval, err := cbg.ReadString(cr)
			if err != nil {
				return err
			}

			name = string(sval)
		}

		switch name {
		// t.DealUUID (uuid.UUID) (array)
		case "DealUUID":

			maj, extra, err = cr.ReadHeader()
			if err != nil {
				return err
			}

			if extra > cbg.ByteArrayMaxLen {
				return fmt.Errorf("t.DealUUID: byte array too large (%d)", extra)
			}
			if maj != cbg.MajByteString {
				return fmt.Errorf("expected byte array")
			}

			if extra != 16 {
				return fmt.Errorf("expected array to have 16 elements")
			}

			t.DealUUID = [16]uint8{}

			if _, err := io.ReadFull(cr, t.DealUUID[:]); err != nil {
				return err
			}
			// t.Signature (crypto.Signature) (struct)
		case "Signature":

			{

				if err := t.Signature.UnmarshalCBOR(cr); err != nil {
					return xerrors.Errorf("unmarshaling t.Signature: %w", err)
				}

			}

		default:
			// Field doesn't exist on this type, so ignore it
			cbg.ScanForLinks(r, func(cid.Cid) {})
		}
	}

	return nil
}
func (t *DealStatusResponse) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}

	cw := cbg.NewCborWriter(w)

	if _, err := cw.Write([]byte{166}); err != nil {
		return err
	}

	// t.DealUUID (uuid.UUID) (array)
	if len("DealUUID") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"DealUUID\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("DealUUID"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("DealUUID")); err != nil {
		return err
	}

	if len(t.DealUUID) > cbg.ByteArrayMaxLen {
		return xerrors.Errorf("Byte array in field t.DealUUID was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajByteString, uint64(len(t.DealUUID))); err != nil {
		return err
	}

	if _, err := cw.Write(t.DealUUID[:]); err != nil {
		return err
	}

	// t.Error (string) (string)
	if len("Error") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"Error\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("Error"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("Error")); err != nil {
		return err
	}

	if len(t.Error) > cbg.MaxLength {
		return xerrors.Errorf("Value in field t.Error was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len(t.Error))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string(t.Error)); err != nil {
		return err
	}

	// t.DealStatus (types.DealStatus) (struct)
	if len("DealStatus") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"DealStatus\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("DealStatus"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("DealStatus")); err != nil {
		return err
	}

	if err := t.DealStatus.MarshalCBOR(cw); err != nil {
		return err
	}

	// t.IsOffline (bool) (bool)
	if len("IsOffline") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"IsOffline\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("IsOffline"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("IsOffline")); err != nil {
		return err
	}

	if err := cbg.WriteBool(w, t.IsOffline); err != nil {
		return err
	}

	// t.TransferSize (uint64) (uint64)
	if len("TransferSize") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"TransferSize\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("TransferSize"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("TransferSize")); err != nil {
		return err
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajUnsignedInt, uint64(t.TransferSize)); err != nil {
		return err
	}

	// t.NBytesReceived (uint64) (uint64)
	if len("NBytesReceived") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"NBytesReceived\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("NBytesReceived"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("NBytesReceived")); err != nil {
		return err
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajUnsignedInt, uint64(t.NBytesReceived)); err != nil {
		return err
	}

	return nil
}

func (t *DealStatusResponse) UnmarshalCBOR(r io.Reader) (err error) {
	*t = DealStatusResponse{}

	cr := cbg.NewCborReader(r)

	maj, extra, err := cr.ReadHeader()
	if err != nil {
		return err
	}
	defer func() {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
	}()

	if maj != cbg.MajMap {
		return fmt.Errorf("cbor input should be of type map")
	}

	if extra > cbg.MaxLength {
		return fmt.Errorf("DealStatusResponse: map struct too large (%d)", extra)
	}

	var name string
	n := extra

	for i := uint64(0); i < n; i++ {

		{
			sval, err := cbg.ReadString(cr)
			if err != nil {
				return err
			}

			name = string(sval)
		}

		switch name {
		// t.DealUUID (uuid.UUID) (array)
		case "DealUUID":

			maj, extra, err = cr.ReadHeader()
			if err != nil {
				return err
			}

			if extra > cbg.ByteArrayMaxLen {
				return fmt.Errorf("t.DealUUID: byte array too large (%d)", extra)
			}
			if maj != cbg.MajByteString {
				return fmt.Errorf("expected byte array")
			}

			if extra != 16 {
				return fmt.Errorf("expected array to have 16 elements")
			}

			t.DealUUID = [16]uint8{}

			if _, err := io.ReadFull(cr, t.DealUUID[:]); err != nil {
				return err
			}
			// t.Error (string) (string)
		case "Error":

			{
				sval, err := cbg.ReadString(cr)
				if err != nil {
					return err
				}

				t.Error = string(sval)
			}
			// t.DealStatus (types.DealStatus) (struct)
		case "DealStatus":

			{

				b, err := cr.ReadByte()
				if err != nil {
					return err
				}
				if b != cbg.CborNull[0] {
					if err := cr.UnreadByte(); err != nil {
						return err
					}
					t.DealStatus = new(DealStatus)
					if err := t.DealStatus.UnmarshalCBOR(cr); err != nil {
						return xerrors.Errorf("unmarshaling t.DealStatus pointer: %w", err)
					}
				}

			}
			// t.IsOffline (bool) (bool)
		case "IsOffline":

			maj, extra, err = cr.ReadHeader()
			if err != nil {
				return err
			}
			if maj != cbg.MajOther {
				return fmt.Errorf("booleans must be major type 7")
			}
			switch extra {
			case 20:
				t.IsOffline = false
			case 21:
				t.IsOffline = true
			default:
				return fmt.Errorf("booleans are either major type 7, value 20 or 21 (got %d)", extra)
			}
			// t.TransferSize (uint64) (uint64)
		case "TransferSize":

			{

				maj, extra, err = cr.ReadHeader()
				if err != nil {
					return err
				}
				if maj != cbg.MajUnsignedInt {
					return fmt.Errorf("wrong type for uint64 field")
				}
				t.TransferSize = uint64(extra)

			}
			// t.NBytesReceived (uint64) (uint64)
		case "NBytesReceived":

			{

				maj, extra, err = cr.ReadHeader()
				if err != nil {
					return err
				}
				if maj != cbg.MajUnsignedInt {
					return fmt.Errorf("wrong type for uint64 field")
				}
				t.NBytesReceived = uint64(extra)

			}

		default:
			// Field doesn't exist on this type, so ignore it
			cbg.ScanForLinks(r, func(cid.Cid) {})
		}
	}

	return nil
}
func (t *DealStatus) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}

	cw := cbg.NewCborWriter(w)

	if _, err := cw.Write([]byte{167}); err != nil {
		return err
	}

	// t.Error (string) (string)
	if len("Error") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"Error\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("Error"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("Error")); err != nil {
		return err
	}

	if len(t.Error) > cbg.MaxLength {
		return xerrors.Errorf("Value in field t.Error was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len(t.Error))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string(t.Error)); err != nil {
		return err
	}

	// t.Status (string) (string)
	if len("Status") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"Status\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("Status"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("Status")); err != nil {
		return err
	}

	if len(t.Status) > cbg.MaxLength {
		return xerrors.Errorf("Value in field t.Status was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len(t.Status))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string(t.Status)); err != nil {
		return err
	}

	// t.SealingStatus (string) (string)
	if len("SealingStatus") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"SealingStatus\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("SealingStatus"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("SealingStatus")); err != nil {
		return err
	}

	if len(t.SealingStatus) > cbg.MaxLength {
		return xerrors.Errorf("Value in field t.SealingStatus was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len(t.SealingStatus))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string(t.SealingStatus)); err != nil {
		return err
	}

	// t.Proposal (market.DealProposal) (struct)
	if len("Proposal") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"Proposal\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("Proposal"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("Proposal")); err != nil {
		return err
	}

	if err := t.Proposal.MarshalCBOR(cw); err != nil {
		return err
	}

	// t.SignedProposalCid (cid.Cid) (struct)
	if len("SignedProposalCid") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"SignedProposalCid\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("SignedProposalCid"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("SignedProposalCid")); err != nil {
		return err
	}

	if err := cbg.WriteCid(cw, t.SignedProposalCid); err != nil {
		return xerrors.Errorf("failed to write cid field t.SignedProposalCid: %w", err)
	}

	// t.PublishCid (cid.Cid) (struct)
	if len("PublishCid") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"PublishCid\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("PublishCid"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("PublishCid")); err != nil {
		return err
	}

	if t.PublishCid == nil {
		if _, err := cw.Write(cbg.CborNull); err != nil {
			return err
		}
	} else {
		if err := cbg.WriteCid(cw, *t.PublishCid); err != nil {
			return xerrors.Errorf("failed to write cid field t.PublishCid: %w", err)
		}
	}

	// t.ChainDealID (abi.DealID) (uint64)
	if len("ChainDealID") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"ChainDealID\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("ChainDealID"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("ChainDealID")); err != nil {
		return err
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajUnsignedInt, uint64(t.ChainDealID)); err != nil {
		return err
	}

	return nil
}

func (t *DealStatus) UnmarshalCBOR(r io.Reader) (err error) {
	*t = DealStatus{}

	cr := cbg.NewCborReader(r)

	maj, extra, err := cr.ReadHeader()
	if err != nil {
		return err
	}
	defer func() {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
	}()

	if maj != cbg.MajMap {
		return fmt.Errorf("cbor input should be of type map")
	}

	if extra > cbg.MaxLength {
		return fmt.Errorf("DealStatus: map struct too large (%d)", extra)
	}

	var name string
	n := extra

	for i := uint64(0); i < n; i++ {

		{
			sval, err := cbg.ReadString(cr)
			if err != nil {
				return err
			}

			name = string(sval)
		}

		switch name {
		// t.Error (string) (string)
		case "Error":

			{
				sval, err := cbg.ReadString(cr)
				if err != nil {
					return err
				}

				t.Error = string(sval)
			}
			// t.Status (string) (string)
		case "Status":

			{
				sval, err := cbg.ReadString(cr)
				if err != nil {
					return err
				}

				t.Status = string(sval)
			}
			// t.SealingStatus (string) (string)
		case "SealingStatus":

			{
				sval, err := cbg.ReadString(cr)
				if err != nil {
					return err
				}

				t.SealingStatus = string(sval)
			}
			// t.Proposal (market.DealProposal) (struct)
		case "Proposal":

			{

				if err := t.Proposal.UnmarshalCBOR(cr); err != nil {
					return xerrors.Errorf("unmarshaling t.Proposal: %w", err)
				}

			}
			// t.SignedProposalCid (cid.Cid) (struct)
		case "SignedProposalCid":

			{

				c, err := cbg.ReadCid(cr)
				if err != nil {
					return xerrors.Errorf("failed to read cid field t.SignedProposalCid: %w", err)
				}

				t.SignedProposalCid = c

			}
			// t.PublishCid (cid.Cid) (struct)
		case "PublishCid":

			{

				b, err := cr.ReadByte()
				if err != nil {
					return err
				}
				if b != cbg.CborNull[0] {
					if err := cr.UnreadByte(); err != nil {
						return err
					}

					c, err := cbg.ReadCid(cr)
					if err != nil {
						return xerrors.Errorf("failed to read cid field t.PublishCid: %w", err)
					}

					t.PublishCid = &c
				}

			}
			// t.ChainDealID (abi.DealID) (uint64)
		case "ChainDealID":

			{

				maj, extra, err = cr.ReadHeader()
				if err != nil {
					return err
				}
				if maj != cbg.MajUnsignedInt {
					return fmt.Errorf("wrong type for uint64 field")
				}
				t.ChainDealID = abi.DealID(extra)

			}

		default:
			// Field doesn't exist on this type, so ignore it
			cbg.ScanForLinks(r, func(cid.Cid) {})
		}
	}

	return nil
}
